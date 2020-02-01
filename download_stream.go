package gridfs

import (
	"context"
	"errors"
	"io"
	"math"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// DownloadStream is a io.ReadSeeker that can be used to download a file from a GridFS bucket.
type DownloadStream struct {
	cursor        *mongo.Cursor
	chunksColl    *mongo.Collection // collection to store file chunks
	filesColl     *mongo.Collection // collection to store file metadata
	file          *File
	buffer        []byte // store up to 1 chunk if the user provided buffer isn't big enough
	bufferStart   int
	bufferEnd     int
	closed        bool
	done          bool
	expectedChunk int32 // index of next expected chunk
	numChunks     int32
	offset        int64
	readDeadline  time.Time
}

func newDownloadStream(file *File, chunksColl, filesColl *mongo.Collection) *DownloadStream {
	chunkSize := file.ChunkSize
	numChunks := int32(math.Ceil(float64(file.Length) / float64(chunkSize)))

	return &DownloadStream{
		chunksColl: chunksColl,
		filesColl:  filesColl,
		file:       file,
		buffer:     make([]byte, chunkSize),
		done:       true,
		numChunks:  numChunks,
	}
}

// SetReadDeadline sets the read deadline for this download stream.
func (ds *DownloadStream) SetReadDeadline(t time.Time) error {
	if ds.closed {
		return ErrStreamClosed
	}

	ds.readDeadline = t
	return nil
}

// Close closes this download stream.
func (ds *DownloadStream) Close() error {
	if ds.closed {
		return ErrStreamClosed
	}

	ds.closed = true
	return nil
}

// Read reads the file from the server and writes it to a destination byte slice.
func (ds *DownloadStream) Read(p []byte) (int, error) {
	if ds.closed {
		return 0, ErrStreamClosed
	}

	if ds.offset == ds.file.Length {
		return 0, io.EOF
	}

	ctx, cancel := deadlineContext(ds.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	if ds.done {
		cursor, err := ds.findChunks(ctx, ds.file.ID, ds.expectedChunk)
		if err != nil {
			return 0, err
		}
		ds.cursor = cursor
		// ds.done = cursor == nil
	}

	bytesCopied := 0
	var err error
	for bytesCopied < len(p) {
		if ds.bufferStart >= ds.bufferEnd {
			// Buffer is empty and can load in data from new chunk.
			err = ds.fillBuffer(ctx)
			if err != nil {
				if err == errNoMoreChunks {
					if bytesCopied == 0 {
						ds.offset = ds.file.Length
						// ds.done = true
						return 0, io.EOF
					}
					return bytesCopied, nil
				}
				return bytesCopied, err
			}
		}

		copied := copy(p[bytesCopied:], ds.buffer[ds.bufferStart:ds.bufferEnd])

		bytesCopied += copied
		ds.bufferStart += copied
		ds.offset += int64(copied)
	}

	return len(p), nil
}

// Seek sets the offset for the next Read or Write on file to
// offset, interpreted according to whence: 0 means relative to
// the origin of the file, 1 means relative to the current offset,
// and 2 means relative to the end. It returns the new offset and
// an Error, if any.
func (ds *DownloadStream) Seek(offset int64, whence int) (pos int64, err error) {
	if ds.closed {
		return ds.offset, ErrStreamClosed
	}

	switch whence {
	case os.SEEK_SET:
	case os.SEEK_CUR:
		offset += ds.offset
	case os.SEEK_END:
		offset += ds.file.Length
	default:
		return ds.offset, errors.New("unsupported whence value")
	}
	if offset > ds.file.Length {
		return ds.offset, errors.New("seek past end of file")
	}

	ds.offset = offset
	ds.expectedChunk = int32(math.Floor(float64(offset) / float64(ds.file.ChunkSize)))
	ds.bufferStart = int(offset) - int(ds.expectedChunk)*int(ds.file.ChunkSize)
	ds.bufferEnd = 0
	ds.done = true

	return ds.offset, nil
}

func (ds *DownloadStream) fillBuffer(ctx context.Context) error {
	if !ds.cursor.Next(ctx) {
		// ds.done = true
		return errNoMoreChunks
	}

	chunkIndex, err := ds.cursor.Current.LookupErr("n")
	if err != nil {
		return err
	}

	if chunkIndex.Int32() != ds.expectedChunk {
		return ErrWrongIndex
	}

	ds.expectedChunk++
	data, err := ds.cursor.Current.LookupErr("data")
	if err != nil {
		return err
	}

	_, dataBytes := data.Binary()
	copied := copy(ds.buffer, dataBytes)

	bytesLen := int32(len(dataBytes))
	if ds.expectedChunk == ds.numChunks {
		// final chunk can be fewer than ds.chunkSize bytes
		bytesDownloaded := int64(ds.file.ChunkSize) * (int64(ds.expectedChunk) - int64(1))
		bytesRemaining := ds.file.Length - int64(bytesDownloaded)

		if int64(bytesLen) != bytesRemaining {
			return ErrWrongSize
		}
	} else if bytesLen != ds.file.ChunkSize {
		// all intermediate chunks must have size ds.chunkSize
		return ErrWrongSize
	}

	if ds.done {
		ds.done = false
	} else {
		ds.bufferStart = 0
	}
	ds.bufferEnd = copied

	return nil
}

func (ds *DownloadStream) findChunks(ctx context.Context, fileID interface{}, n int32) (*mongo.Cursor, error) {
	id, err := convertFileID(fileID)
	if err != nil {
		return nil, err
	}
	chunksCursor, err := ds.chunksColl.Find(ctx,
		bson.M{"$and": bson.A{bson.M{"files_id": id}, bson.M{"n": bson.M{"$gte": n}}}},
		options.Find().SetSort(bsonx.Doc{bsonx.Elem{Key: "n", Value: bsonx.Int32(1)}})) // sort by chunk index
	if err != nil {
		return nil, err
	}

	return chunksCursor, nil
}
