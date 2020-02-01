package gridfs

import (
	"bufio"
	"context"
	"crypto/md5"
	"io"
	"log"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// UploadBufferSize is the size in bytes of one stream batch. Chunks will be written to the db after the sum of chunk
// lengths is equal to the batch size.
const UploadBufferSize = 16 * 1024 * 1024 // 16 MiB

// UploadStream is used to upload files in chunks.
type UploadStream struct {
	chunkIndex    int
	bufferIndex   int
	file          *File
	cursor        *mongo.Cursor
	chunksColl    *mongo.Collection // collection to store file chunks
	filesColl     *mongo.Collection // collection to store file metadata
	buffer        []byte            // store up to 1 chunk if the user provided buffer isn't big enough
	bufferStart   int
	bufferEnd     int
	closed        bool
	done          bool
	expectedChunk int32 // index of next expected chunk
	numChunks     int32
	offset        int64
	readDeadline  time.Time
	writeDeadline time.Time
}

// NewUploadStream creates a new upload stream.
func newUploadStream(file *File, chunksColl, filesColl *mongo.Collection) *UploadStream {
	numChunks := int32(math.Ceil(float64(file.Length) / float64(file.ChunkSize)))

	return &UploadStream{
		chunksColl: chunksColl,
		filesColl:  filesColl,
		file:       file,
		buffer:     make([]byte, UploadBufferSize),
		done:       true,
		numChunks:  numChunks,
	}
}

// SetReadDeadline sets the read deadline for this download stream.
func (us *UploadStream) SetReadDeadline(t time.Time) error {
	if us.closed {
		return ErrStreamClosed
	}

	us.readDeadline = t
	return nil
}

// SetWriteDeadline sets the write deadline for this stream.
func (us *UploadStream) SetWriteDeadline(t time.Time) error {
	if us.closed {
		return ErrStreamClosed
	}

	us.writeDeadline = t
	return nil
}

// Close closes this upload stream.
func (us *UploadStream) Close() error {
	if us.closed {
		return ErrStreamClosed
	}

	ctx, cancel := deadlineContext(us.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	if us.bufferIndex != 0 {
		if err := us.uploadChunks(ctx, true); err != nil {
			return err
		}
	}

	us.numChunks = int32(math.Ceil(float64(us.file.Length) / float64(us.file.ChunkSize)))
	r := bufio.NewReader(us)
	h := md5.New()
	if _, err := io.Copy(h, r); err != nil {
		log.Println(err)
	}
	log.Printf("%x", h.Sum(nil))

	if err := us.createFilesCollDoc(ctx); err != nil {
		return err
	}

	us.closed = true
	return nil
}

// Read reads the file from the server and writes it to a destination byte slice.
func (us *UploadStream) Read(p []byte) (int, error) {
	if us.closed {
		return 0, ErrStreamClosed
	}

	if us.offset == us.file.Length {
		return 0, io.EOF
	}

	ctx, cancel := deadlineContext(us.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	if us.done {
		cursor, err := us.findChunks(ctx, us.file.ID, us.expectedChunk)
		if err != nil {
			return 0, err
		}
		us.cursor = cursor
		us.done = cursor == nil
	}

	bytesCopied := 0
	var err error
	for bytesCopied < len(p) {
		if us.bufferStart >= us.bufferEnd {
			// Buffer is empty and can load in data from new chunk.
			err = us.fillBuffer(ctx)
			if err != nil {
				if err == errNoMoreChunks {
					if bytesCopied == 0 {
						us.offset = us.file.Length
						// us.done = true
						return 0, io.EOF
					}
					return bytesCopied, nil
				}
				return bytesCopied, err
			}
		}

		copied := copy(p[bytesCopied:], us.buffer[us.bufferStart:us.bufferEnd])

		bytesCopied += copied
		us.bufferStart += copied
		us.offset += int64(copied)
	}

	return len(p), nil
}

// Write transfers the contents of a byte slice into this upload stream. If the stream's underlying buffer fills up,
// the buffer will be uploaded as chunks to the server. Implements the io.Writer interface.
func (us *UploadStream) Write(p []byte) (int, error) {
	if us.closed {
		return 0, ErrStreamClosed
	}

	var ctx context.Context

	ctx, cancel := deadlineContext(us.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	origLen := len(p)
	for {
		if len(p) == 0 {
			break
		}

		n := copy(us.buffer[us.bufferIndex:], p) // copy as much as possible
		p = p[n:]
		us.bufferIndex += n

		if us.bufferIndex == UploadBufferSize {
			err := us.uploadChunks(ctx, false)
			if err != nil {
				return 0, err
			}
		}
	}
	return origLen, nil
}

// Abort closes the stream and deletes all file chunks that have already been written.
func (us *UploadStream) Abort() error {
	if us.closed {
		return ErrStreamClosed
	}

	ctx, cancel := deadlineContext(us.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	id, err := convertFileID(us.file.ID)
	if err != nil {
		return err
	}
	_, err = us.chunksColl.DeleteMany(ctx, bsonx.Doc{bsonx.Elem{Key: "files_id", Value: id}})
	if err != nil {
		return err
	}

	us.closed = true
	return nil
}

// uploadChunks uploads the current buffer as a series of chunks to the bucket
// if uploadPartial is true, any data at the end of the buffer that is smaller than a chunk will be uploaded as a partial
// chunk. if it is false, the data will be moved to the front of the buffer.
// uploadChunks sets us.bufferIndex to the next available index in the buffer after uploading
func (us *UploadStream) uploadChunks(ctx context.Context, uploadPartial bool) error {
	chunks := float64(us.bufferIndex) / float64(us.file.ChunkSize)
	numChunks := int(math.Ceil(chunks))
	if !uploadPartial {
		numChunks = int(math.Floor(chunks))
	}

	docs := make([]interface{}, int(numChunks))

	id, err := convertFileID(us.file.ID)
	if err != nil {
		return err
	}
	begChunkIndex := us.chunkIndex
	for i := 0; i < us.bufferIndex; i += int(us.file.ChunkSize) {
		endIndex := i + int(us.file.ChunkSize)
		if us.bufferIndex-i < int(us.file.ChunkSize) {
			// partial chunk
			if !uploadPartial {
				break
			}
			endIndex = us.bufferIndex
		}
		chunkData := us.buffer[i:endIndex]
		docs[us.chunkIndex-begChunkIndex] = bsonx.Doc{
			bsonx.Elem{Key: "_id", Value: bsonx.ObjectID(primitive.NewObjectID())},
			bsonx.Elem{Key: "files_id", Value: id},
			bsonx.Elem{Key: "n", Value: bsonx.Int32(int32(us.chunkIndex))},
			bsonx.Elem{Key: "data", Value: bsonx.Binary(0x00, chunkData)},
		}
		us.chunkIndex++
		us.file.Length += int64(len(chunkData))
	}

	_, err = us.chunksColl.InsertMany(ctx, docs)
	if err != nil {
		return err
	}

	// copy any remaining bytes to beginning of buffer and set buffer index
	bytesUploaded := numChunks * int(us.file.ChunkSize)
	if bytesUploaded != UploadBufferSize && !uploadPartial {
		copy(us.buffer[0:], us.buffer[bytesUploaded:us.bufferIndex])
	}
	us.bufferIndex = UploadBufferSize - bytesUploaded
	return nil
}

func (us *UploadStream) createFilesCollDoc(ctx context.Context) error {
	id, err := convertFileID(us.file.ID)
	if err != nil {
		return err
	}
	doc := bsonx.Doc{
		bsonx.Elem{Key: "_id", Value: id},
		bsonx.Elem{Key: "length", Value: bsonx.Int64(us.file.Length)},
		bsonx.Elem{Key: "chunkSize", Value: bsonx.Int32(us.file.ChunkSize)},
		bsonx.Elem{Key: "uploadDate", Value: bsonx.DateTime(time.Now().UnixNano() / int64(time.Millisecond))},
		bsonx.Elem{Key: "filename", Value: bsonx.String(us.file.Filename)},
	}

	if us.file.Metadata != nil {
		doc = append(doc, bsonx.Elem{Key: "metadata", Value: bsonx.Document(us.file.Metadata)})
	}

	_, err = us.filesColl.InsertOne(ctx, doc)
	if err != nil {
		return err
	}

	return nil
}

func (us *UploadStream) fillBuffer(ctx context.Context) error {
	if !us.cursor.Next(ctx) {
		// us.done = true
		return errNoMoreChunks
	}

	chunkIndex, err := us.cursor.Current.LookupErr("n")
	if err != nil {
		return err
	}

	if chunkIndex.Int32() != us.expectedChunk {
		return ErrWrongIndex
	}

	us.expectedChunk++
	data, err := us.cursor.Current.LookupErr("data")
	if err != nil {
		return err
	}

	_, dataBytes := data.Binary()
	copied := copy(us.buffer, dataBytes)

	bytesLen := int32(len(dataBytes))
	if us.expectedChunk == us.numChunks {
		// final chunk can be fewer than us.chunkSize bytes
		bytesDownloaded := int64(us.file.ChunkSize) * (int64(us.expectedChunk) - int64(1))
		bytesRemaining := us.file.Length - int64(bytesDownloaded)

		if int64(bytesLen) != bytesRemaining {
			return ErrWrongSize
		}
	} else if bytesLen != us.file.ChunkSize {
		// all intermediate chunks must have size us.chunkSize
		return ErrWrongSize
	}

	us.bufferStart = 0
	us.bufferEnd = copied

	return nil
}

func (us *UploadStream) findChunks(ctx context.Context, fileID interface{}, n int32) (*mongo.Cursor, error) {
	id, err := convertFileID(fileID)
	if err != nil {
		return nil, err
	}
	chunksCursor, err := us.chunksColl.Find(ctx,
		bson.M{"$and": bson.A{bson.M{"files_id": id}, bson.M{"n": bson.M{"$gte": n}}}},
		options.Find().SetSort(bsonx.Doc{bsonx.Elem{Key: "n", Value: bsonx.Int32(1)}})) // sort by chunk index
	if err != nil {
		return nil, err
	}

	return chunksCursor, nil
}
