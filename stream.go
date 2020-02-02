package gridfs

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

type stream struct {
	closed    bool
	offset    int64
	numChunks int32

	readExpectedChunk int32 // index of next expected chunk
	readCursorChunk   int32
	readCursor        *mongo.Cursor
	readBuffer        []byte
	readBufferStart   int
	readBufferEnd     int
	readDeadline      time.Time

	writeStartChunk    int32
	writeExpectedChunk int32
	writeBuffer        []byte
	writeBufferStart   int
	writeBufferEnd     int
	writeDeadline      time.Time

	fileInfo   *FileInfo
	chunksColl *mongo.Collection // The collection to store file chunks
	filesColl  *mongo.Collection // The collection to store file metadata
	tempsColl  *mongo.Collection // The collection to store file temps.
}

func newStream(fileInfo *FileInfo, chunksColl, filesColl, tempsColl *mongo.Collection) *stream {
	numChunks := int32(math.Ceil(float64(fileInfo.Length) / float64(fileInfo.ChunkSize)))

	return &stream{
		numChunks:       numChunks,
		readCursorChunk: int32(-1),
		fileInfo:        fileInfo,
		chunksColl:      chunksColl,
		filesColl:       filesColl,
		tempsColl:       tempsColl,
	}
}

// SetWriteDeadline sets the write deadline for this bucket.
func (s *stream) setWriteDeadline(t time.Time) error {
	if s.closed {
		return ErrStreamClosed
	}
	s.writeDeadline = t
	return nil
}

// SetReadDeadline sets the read deadline for this bucket
func (s *stream) setReadDeadline(t time.Time) error {
	if s.closed {
		return ErrStreamClosed
	}
	s.readDeadline = t
	return nil
}

func (s *stream) close() error {
	if s.closed {
		return ErrStreamClosed
	}

	ctx, cancel := deadlineContext(s.writeDeadline)
	if cancel != nil {
		defer cancel()
	}
	if s.writeBufferEnd != 0 {
		if err := s.uploadChunks(ctx, true); err != nil {
			return err
		}
	}
	if err := s.createFilesCollDoc(ctx); err != nil {
		return err
	}

	s.closed = true
	return nil
}

// read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
func (s *stream) read(b []byte) (int, error) {
	if s.closed {
		return 0, ErrStreamClosed
	}

	if s.offset == s.fileInfo.Length {
		return 0, io.EOF
	}

	if s.readBuffer == nil {
		s.readBuffer = make([]byte, DefaultReadBufferSize)
	}

	ctx, cancel := deadlineContext(s.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	// if s.readCursorChunk == -1 || s.readCursorChunk != s.readExpectedChunk-1 {
	// 	cursor, err := s.findChunks(ctx, s.fileInfo.ID, s.readExpectedChunk)
	// 	if err != nil {
	// 		return 0, err
	// 	}
	// 	s.readCursor = cursor
	// }

	bytesCopied := 0
	var err error
	for bytesCopied < len(b) {
		if s.readBufferStart >= s.readBufferEnd {
			// Buffer is empty and can load in data from new chunk.
			err = s.fillBuffer(ctx)
			if err != nil {
				if err == errNoMoreChunks {
					if bytesCopied == 0 {
						return 0, io.EOF
					}
					return bytesCopied, nil
				}
				return bytesCopied, err
			}
		}

		copied := copy(b[bytesCopied:], s.readBuffer[s.readBufferStart:s.readBufferEnd])

		s.offset += int64(copied)
		s.readBufferStart += copied
		bytesCopied += copied
	}

	return len(b), nil
}

// pread reads len(b) bytes from the File starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// EOF is signaled by a zero count with err set to 0.
func (s *stream) pread(b []byte, off int64) (int, error) {
	if s.closed {
		return 0, ErrStreamClosed
	}
	_, err := s.seek(off, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return s.read(b)
}

// write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
func (s *stream) write(b []byte) (int, error) {
	if s.closed {
		return 0, ErrStreamClosed
	}

	if s.writeBuffer == nil {
		s.writeBuffer = make([]byte, DefaultWriteBufferSize)
	}

	ctx, cancel := deadlineContext(s.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	origLen := len(b)
	for {
		if len(b) == 0 {
			break
		}

		n := copy(s.writeBuffer[s.writeBufferEnd:], b) // copy as much as possible
		b = b[n:]
		s.offset += int64(n)
		s.writeBufferEnd += n

		if s.writeBufferEnd == DefaultWriteBufferSize {
			err := s.uploadChunks(ctx, false)
			if err != nil {
				return 0, err
			}
		}
	}
	return origLen, nil
}

// pwrite writes len(b) bytes to the File starting at byte offset off.
// It returns the number of bytes written and an error, if any.
func (s *stream) pwrite(b []byte, off int64) (int, error) {
	if s.closed {
		return 0, ErrStreamClosed
	}
	_, err := s.seek(off, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return s.write(b)
}

// seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (s *stream) seek(offset int64, whence int) (int64, error) {
	if s.closed {
		return 0, ErrStreamClosed
	}

	switch whence {
	case io.SeekStart:
	case os.SEEK_CUR:
		offset += s.offset
	case os.SEEK_END:
		offset += s.fileInfo.Length
	default:
		return s.offset, errors.New("unsupported whence value")
	}

	if offset > s.fileInfo.Length {
		return s.offset, errors.New("seek past end of file")
	}

	s.offset = offset
	expectedChunk := float64(offset) / float64(s.fileInfo.ChunkSize)

	readExpectedChunk := int32(math.Floor(expectedChunk))
	s.readBufferStart = int(offset) - int(readExpectedChunk)*int(s.fileInfo.ChunkSize)
	if !(s.readCursorChunk == readExpectedChunk && s.readBufferEnd > s.readBufferStart) {
		s.readExpectedChunk = readExpectedChunk
		s.readBufferEnd = 0
	}

	s.writeStartChunk = readExpectedChunk
	s.writeExpectedChunk = s.writeStartChunk
	s.writeBufferStart = int(int32(math.Ceil(expectedChunk)))*int(s.fileInfo.ChunkSize) - int(offset)
	s.writeBufferEnd = 0

	return s.offset, nil
}

// uploadChunks uploads the current buffer as a series of chunks to the bucket
// if uploadPartial is true, any data at the end of the buffer that is smaller than a chunk will be uploaded as a partial
// chunk. if it is false, the data will be moved to the front of the buffer.
// uploadChunks sets s.writeBufferEnd to the next available index in the buffer after uploading
func (s *stream) uploadChunks(ctx context.Context, uploadPartial bool) error {
	chunks := math.Max(float64(s.writeBufferEnd-s.writeBufferStart)/float64(s.fileInfo.ChunkSize), 0)
	numChunks := int(math.Ceil(chunks))
	if !uploadPartial {
		numChunks = int(math.Floor(chunks))
	}

	id, err := convertFileID(s.fileInfo.ID)
	if err != nil {
		return err
	}

	if numChunks != 0 {
		docs := make([]interface{}, int(numChunks))

		begWriteExpectedChunk := s.writeExpectedChunk
		for i := s.writeBufferStart; i < s.writeBufferEnd; i += int(s.fileInfo.ChunkSize) {
			endIndex := i + int(s.fileInfo.ChunkSize)
			if s.writeBufferEnd-i < int(s.fileInfo.ChunkSize) {
				// partial chunk
				if !uploadPartial {
					break
				}
				endIndex = s.writeBufferEnd
			}
			chunkData := s.writeBuffer[i:endIndex]
			docs[s.writeExpectedChunk-begWriteExpectedChunk] = bsonx.Doc{
				bsonx.Elem{Key: "_id", Value: bsonx.ObjectID(primitive.NewObjectID())},
				bsonx.Elem{Key: "files_id", Value: id},
				bsonx.Elem{Key: "n", Value: bsonx.Int32(int32(s.writeExpectedChunk))},
				bsonx.Elem{Key: "data", Value: bsonx.Binary(0x00, chunkData)},
			}
			s.writeExpectedChunk++
			s.fileInfo.Length += int64(len(chunkData))
		}

		_, err = s.chunksColl.InsertMany(ctx, docs)
		if err != nil {
			return err
		}

		// copy any remaining bytes to beginning of buffer and set buffer index
		bytesUploaded := numChunks * int(s.fileInfo.ChunkSize)
		if bytesUploaded != DefaultWriteBufferSize && !uploadPartial {
			copy(s.writeBuffer[s.writeBufferStart:], s.writeBuffer[s.writeBufferStart+bytesUploaded:s.writeBufferEnd])
		}
		s.writeBufferEnd = s.writeBufferEnd - bytesUploaded
	}

	if s.writeBufferStart != 0 {
		length := int32(s.writeBufferStart)
		from := s.fileInfo.ChunkSize - length
		to := s.fileInfo.ChunkSize
		if s.writeBufferStart > s.writeBufferEnd {
			length = int32(s.writeBufferEnd)
			to = from + length
		}

		chunkData := s.writeBuffer[0:length]
		doc := bsonx.Doc{
			bsonx.Elem{Key: "_id", Value: bsonx.ObjectID(primitive.NewObjectID())},
			bsonx.Elem{Key: "files_id", Value: id},
			bsonx.Elem{Key: "n", Value: bsonx.Int32(s.writeStartChunk)},
			bsonx.Elem{Key: "length", Value: bsonx.Int32(length)},
			bsonx.Elem{Key: "from", Value: bsonx.Int32(from)},
			bsonx.Elem{Key: "to", Value: bsonx.Int32(to)},
			bsonx.Elem{Key: "data", Value: bsonx.Binary(0x00, chunkData)},
		}

		s.fileInfo.Length += int64(len(chunkData))

		_, err = s.tempsColl.InsertOne(ctx, doc)
		if err != nil {
			return err
		}

		copy(s.writeBuffer[0:], s.writeBuffer[s.writeBufferStart:s.writeBufferEnd])
		s.readBufferStart = 0
		s.writeBufferEnd = s.writeBufferEnd - s.readBufferStart
	}

	return nil
}

func (s *stream) fillBuffer(ctx context.Context) error {
	cursor, err := s.findChunks(ctx, s.fileInfo.ID, s.readExpectedChunk)
	if err != nil {
		return err
	}
	s.readCursor = cursor

	if !s.readCursor.Next(ctx) {
		return errNoMoreChunks
	}

	chunkIndex, err := s.readCursor.Current.LookupErr("n")
	if err != nil {
		return err
	}

	if chunkIndex.Int32() != s.readExpectedChunk {
		return ErrWrongIndex
	}

	s.readExpectedChunk++
	data, err := s.readCursor.Current.LookupErr("data")
	if err != nil {
		return err
	}

	_, dataBytes := data.Binary()
	copied := copy(s.readBuffer, dataBytes)

	bytesLen := int32(len(dataBytes))
	if s.readExpectedChunk == s.numChunks {
		// final chunk can be fewer than s.chunkSize bytes
		bytesDownloaded := int64(s.fileInfo.ChunkSize) * (int64(s.readExpectedChunk) - int64(1))
		bytesRemaining := s.fileInfo.Length - int64(bytesDownloaded)

		if int64(bytesLen) != bytesRemaining {
			return ErrWrongSize
		}
	} else if bytesLen != s.fileInfo.ChunkSize {
		// all intermediate chunks must have size s.chunkSize
		return ErrWrongSize
	}

	if s.readCursorChunk != -1 && s.readCursorChunk != chunkIndex.Int32() {
		s.readBufferStart = 0
	}
	s.readCursorChunk = chunkIndex.Int32()
	s.readBufferEnd = copied

	return nil
}

func (s *stream) findChunks(ctx context.Context, fileID interface{}, n int32) (*mongo.Cursor, error) {
	id, err := convertFileID(fileID)
	if err != nil {
		return nil, err
	}
	chunksCursor, err := s.chunksColl.Find(ctx,
		bson.M{"$and": bson.A{bson.M{"files_id": id}, bson.M{"n": n}}},
		options.Find().SetSort(bsonx.Doc{bsonx.Elem{Key: "n", Value: bsonx.Int32(1)}})) // sort by chunk index
	if err != nil {
		return nil, err
	}

	return chunksCursor, nil
}

func (s *stream) createFilesCollDoc(ctx context.Context) error {
	id, err := convertFileID(s.fileInfo.ID)
	if err != nil {
		return err
	}

	doc := bsonx.Doc{
		bsonx.Elem{Key: "_id", Value: id},
		bsonx.Elem{Key: "length", Value: bsonx.Int64(s.fileInfo.Length)},
		bsonx.Elem{Key: "chunkSize", Value: bsonx.Int32(s.fileInfo.ChunkSize)},
		bsonx.Elem{Key: "uploadDate", Value: bsonx.DateTime(time.Now().UnixNano() / int64(time.Millisecond))},
		bsonx.Elem{Key: "filename", Value: bsonx.String(s.fileInfo.Filename)},
	}

	if s.fileInfo.Metadata != nil {
		doc = append(doc, bsonx.Elem{Key: "metadata", Value: bsonx.Document(s.fileInfo.Metadata)})
	}

	if s.fileInfo.Reference != nil {
		doc = append(doc, bsonx.Elem{Key: "reference", Value: bsonx.Document(s.fileInfo.Reference)})
	}

	_ = s.filesColl.FindOneAndReplace(ctx, bsonx.Doc{bsonx.Elem{Key: "_id", Value: id}}, doc)
	// _, err = s.filesColl.InsertOne(ctx, doc)
	// if err != nil {
	// 	return err
	// }

	return nil
}
