package gridfs

import (
	"context"
	"io"
	"math"
	"runtime"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

const (
	// StreamCreated equal 1 if stream buffer has been modified
	StreamCreated uint8 = 1 << 0
	// StreamClosed equal 1 if stream closed
	StreamClosed uint8 = 1 << 1
	// StreamRead equal 1 if stream can be read
	StreamRead uint8 = 1 << 2
	// StreamWrite equal 1 if stream can be wrote
	StreamWrite uint8 = 1 << 3
	// StreamModified equal 1 if stream buffer has been modified
	StreamModified uint8 = 1 << 4

	minStreamBufferLength int32 = 510 * 1024       // 510KiB
	maxStreamBufferLength int32 = 32 * 1024 * 1024 // 32MiB
)

type stream struct {
	mu        sync.Mutex
	flag      uint8
	numChunks int32
	offset    int64

	buffer           []byte
	bufferFilled     bool
	bufferStart      int
	bufferEnd        int
	bufferStartChunk int32
	bufferEndChunk   int32
	expectedChunk    int32 // index of next expected chunk

	readDeadline  time.Time
	writeDeadline time.Time

	fileInfo   *FileInfo
	chunksColl *mongo.Collection // The collection to store file chunks
	filesColl  *mongo.Collection // The collection to store file metadata
	tempsColl  *mongo.Collection // The collection to store file temps.
}

func (s *stream) init(flag uint8) {
	s.flag = flag
	s.numChunks = int32(math.Ceil(float64(s.fileInfo.Length) / float64(s.fileInfo.ChunkSize)))
	bufferLength := s.fileInfo.ChunkSize * 8
	if bufferLength < minStreamBufferLength {
		bufferLength = minStreamBufferLength
	} else if bufferLength > maxStreamBufferLength {
		bufferLength = maxStreamBufferLength
	}
	s.buffer = make([]byte, bufferLength)
	ctx, cancel := deadlineContext(s.writeDeadline)
	if cancel != nil {
		defer cancel()
	}
	if s.flag&StreamCreated == 0 {
		s.createFilesCollDoc(ctx)
	}
}

func (s *stream) close() error {
	if s == nil || s.flag&StreamClosed != 0 {
		return ErrStreamClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.flag&StreamModified != 0 {
		ctx, cancel := deadlineContext(s.writeDeadline)
		if cancel != nil {
			defer cancel()
		}
		err := s.writeChunks(ctx, true)
		if err != nil {
			return err
		}
		if err := s.updateFilesCollDoc(ctx); err != nil {
			return err
		}
		s.flag = s.flag &^ StreamModified
	}
	s.flag = s.flag | StreamClosed

	// no need for a finalizer anymore
	runtime.SetFinalizer(s, nil)
	return nil
}

// read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
func (s *stream) read(b []byte) (int, error) {
	if s.flag&StreamRead == 0 {
		return 0, ErrStreamPermission
	}

	if s.offset == s.fileInfo.Length {
		return 0, io.EOF
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := deadlineContext(s.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	bytesCopied := 0
	for bytesCopied < len(b) {
		if s.bufferStart >= s.bufferEnd {
			err := s.readChunks(ctx)
			if err != nil {
				if err == errNoMoreChunks {
					if bytesCopied == 0 {
						return 0, io.EOF
					}
					return bytesCopied, nil
				}
				return bytesCopied, err
			}
		} else {
			copied := copy(b[bytesCopied:], s.buffer[s.bufferStart:s.bufferEnd])

			s.offset += int64(copied)
			s.bufferStart += copied
			s.bufferStartChunk = s.bufferEndChunk
			bytesCopied += copied
		}
	}

	return len(b), nil
}

// pread reads len(b) bytes from the File starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// EOF is signaled by a zero count with err set to nil.
func (s *stream) pread(b []byte, off int64) (int, error) {
	if s.flag&StreamRead == 0 {
		return 0, ErrStreamPermission
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
	if s.flag&StreamWrite == 0 {
		return 0, ErrStreamPermission
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := deadlineContext(s.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	bytesCopied := 0
	for len(b) != 0 {
		if s.bufferEnd == len(s.buffer) {
			err := s.writeChunks(ctx, false)
			if err != nil {
				return 0, err
			}
		} else {
			copied := copy(s.buffer[s.bufferEnd:], b)
			b = b[copied:]

			s.offset += int64(copied)
			s.bufferEnd += copied
			s.flag = s.flag | StreamModified
			bytesCopied += copied
		}
	}

	return bytesCopied, nil
}

// pwrite writes len(b) bytes to the File starting at byte offset off.
// It returns the number of bytes written and an error, if any.
func (s *stream) pwrite(b []byte, off int64) (int, error) {
	if s.flag&StreamWrite == 0 {
		return 0, ErrStreamPermission
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
	if s.flag&(StreamRead|StreamWrite) == 0 {
		return 0, ErrStreamPermission
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.flag&StreamModified != 0 {
		ctx, cancel := deadlineContext(s.writeDeadline)
		if cancel != nil {
			defer cancel()
		}
		err := s.writeChunks(ctx, true)
		if err != nil {
			return 0, err
		}
	}

	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += s.offset
	case io.SeekEnd:
		offset += s.fileInfo.Length
	default:
		return s.offset, ErrStreamSeekUnsupport
	}

	if offset > s.fileInfo.Length {
		return s.offset, ErrStreamSeekOverflow
	}

	s.offset = offset
	expectedChunk := int32(math.Floor(float64(offset) / float64(s.fileInfo.ChunkSize)))
	s.bufferStart = int(offset) - int(expectedChunk)*int(s.fileInfo.ChunkSize)
	s.bufferStartChunk = expectedChunk
	if !s.bufferFilled || !(s.bufferStartChunk == expectedChunk && s.bufferEnd > s.bufferStart) {
		s.bufferFilled = false
		s.bufferEnd = 0
		s.bufferEndChunk = expectedChunk
		s.expectedChunk = expectedChunk
	}

	return s.offset, nil
}

// setDeadline sets the read and write deadline.
func (s *stream) setDeadline(t time.Time) error {
	if s.flag&(StreamRead|StreamWrite) == 0 {
		return ErrStreamPermission
	}
	s.readDeadline = t
	s.writeDeadline = t
	return nil
}

// setReadDeadline sets the read deadline.
func (s *stream) setReadDeadline(t time.Time) error {
	if s.flag&StreamRead == 0 {
		return ErrStreamPermission
	}
	s.readDeadline = t
	return nil
}

// setWriteDeadline sets the write deadline.
func (s *stream) setWriteDeadline(t time.Time) error {
	if s.flag&StreamWrite == 0 {
		return ErrStreamPermission
	}
	s.writeDeadline = t
	return nil
}

// readChunks read chunks from the bucket to the current buffer.
// readChunks sets s.bufferEnd to the next available index in the buffer after read
func (s *stream) readChunks(ctx context.Context) error {
	if s.bufferFilled {
		copy(s.buffer[0:], s.buffer[s.bufferStart:s.bufferEnd])
		s.bufferStartChunk = s.bufferEndChunk
		s.bufferEnd -= s.bufferStart
		s.bufferStart = 0
	}
	chunks := math.Max(float64(len(s.buffer)-s.bufferEnd)/float64(s.fileInfo.ChunkSize), 0)
	numChunks := int64(math.Floor(chunks))
	maxChunks := int64(s.numChunks - s.bufferEndChunk)
	if numChunks > maxChunks {
		numChunks = maxChunks
	}

	if numChunks != 0 {
		id, err := convertFileID(s.fileInfo.ID)
		if err != nil {
			return err
		}
		cursor, err := s.chunksColl.Find(ctx,
			bson.M{"$and": bson.A{bson.M{"files_id": id}, bson.M{"n": bson.M{"$gte": s.expectedChunk}}}},
			options.Find().SetLimit(numChunks).SetSort(bsonx.Doc{bsonx.Elem{Key: "n", Value: bsonx.Int32(1)}})) // sort by chunk index
		if err != nil {
			return err
		}

		for i := int64(0); i < numChunks; i++ {
			if !cursor.Next(ctx) {
				return errNoMoreChunks
			}
			chunkIndex, err := cursor.Current.LookupErr("n")
			if err != nil {
				return err
			}
			if chunkIndex.Int32() != s.expectedChunk {
				return ErrWrongIndex
			}
			s.expectedChunk++
			data, err := cursor.Current.LookupErr("data")
			if err != nil {
				return err
			}
			_, dataBytes := data.Binary()
			copied := copy(s.buffer[s.bufferEnd:], dataBytes)
			bytesLen := int32(len(dataBytes))
			if s.expectedChunk == s.numChunks {
				// final chunk can be fewer than s.chunkSize bytes
				bytesDownloaded := int64(s.fileInfo.ChunkSize) * (int64(s.expectedChunk) - int64(1))
				bytesRemaining := s.fileInfo.Length - int64(bytesDownloaded)

				if int64(bytesLen) != bytesRemaining {
					return ErrWrongSize
				}
			} else if bytesLen != s.fileInfo.ChunkSize {
				// all intermediate chunks must have size s.chunkSize
				return ErrWrongSize
			}
			s.bufferEnd += copied
			s.bufferEndChunk = chunkIndex.Int32()
		}
	}
	s.bufferFilled = true
	return nil
}

// writeChunks uploads the current buffer as a series of chunks to the bucket.
// if partial is true, any data at the end of the buffer that is smaller than a chunk will be wrote as a partial
// chunk. if it is false, the data will be moved to the front of the buffer.
// writeChunks sets s.bufferEnd to the next available index in the buffer after write
func (s *stream) writeChunks(ctx context.Context, partial bool) error {
	chunks := math.Max(float64(s.bufferEnd-s.bufferStart)/float64(s.fileInfo.ChunkSize), 0)
	numChunks := int(math.Ceil(chunks))
	if !partial {
		numChunks = int(math.Floor(chunks))
	}

	id, err := convertFileID(s.fileInfo.ID)
	if err != nil {
		return err
	}

	if numChunks != 0 {
		docs := make([]interface{}, int(numChunks))

		begExpectedChunk := s.expectedChunk
		for i := s.bufferStart; i < s.bufferEnd; i += int(s.fileInfo.ChunkSize) {
			endIndex := i + int(s.fileInfo.ChunkSize)
			if s.bufferEnd-i < int(s.fileInfo.ChunkSize) {
				// partial chunk
				if !partial {
					break
				}
				endIndex = s.bufferEnd
			}
			chunkData := s.buffer[i:endIndex]
			docs[s.expectedChunk-begExpectedChunk] = bsonx.Doc{
				bsonx.Elem{Key: "_id", Value: bsonx.ObjectID(primitive.NewObjectID())},
				bsonx.Elem{Key: "files_id", Value: id},
				bsonx.Elem{Key: "n", Value: bsonx.Int32(int32(s.expectedChunk))},
				bsonx.Elem{Key: "data", Value: bsonx.Binary(0x00, chunkData)},
			}
			s.expectedChunk++
			s.fileInfo.Length += int64(len(chunkData))
		}

		_, err = s.chunksColl.InsertMany(ctx, docs)
		if err != nil {
			return err
		}

		// copy any remaining bytes to beginning of buffer and set buffer index
		bytesUploaded := numChunks * int(s.fileInfo.ChunkSize)
		if bytesUploaded != len(s.buffer) && !partial {
			copy(s.buffer[s.bufferStart:], s.buffer[s.bufferStart+bytesUploaded:s.bufferEnd])
		}
		s.bufferEnd = s.bufferEnd - bytesUploaded
	}

	if s.bufferStart != 0 {
		length := int32(s.bufferStart)
		from := s.fileInfo.ChunkSize - length
		to := s.fileInfo.ChunkSize
		if s.bufferStart > s.bufferEnd {
			length = int32(s.bufferEnd)
			to = from + length
		}

		chunkData := s.buffer[0:length]
		doc := bsonx.Doc{
			bsonx.Elem{Key: "_id", Value: bsonx.ObjectID(primitive.NewObjectID())},
			bsonx.Elem{Key: "files_id", Value: id},
			bsonx.Elem{Key: "n", Value: bsonx.Int32(s.bufferStartChunk)},
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

		copy(s.buffer[0:], s.buffer[s.bufferStart:s.bufferEnd])
		s.bufferEnd = s.bufferEnd - s.bufferStart
		s.bufferStart = 0
	}

	s.bufferEndChunk = int32(math.Floor(float64(s.offset) / float64(s.fileInfo.ChunkSize)))
	s.bufferStartChunk = s.bufferEndChunk

	s.flag = s.flag & ^StreamModified
	return nil
}

func (s *stream) createFilesCollDoc(ctx context.Context) error {
	doc, err := s.fileInfo.Doc()
	if err != nil {
		return err
	}
	_, err = s.filesColl.InsertOne(ctx, doc)
	if err != nil {
		return err
	}
	return nil
}

func (s *stream) updateFilesCollDoc(ctx context.Context) error {
	id, err := s.fileInfo.FileID()
	if err != nil {
		return err
	}
	doc, err := s.fileInfo.Doc()
	if err != nil {
		return err
	}
	singleResult := s.filesColl.FindOneAndReplace(ctx, bsonx.Doc{bsonx.Elem{Key: "_id", Value: id}}, doc)
	if err := singleResult.Err(); err != nil {
		return err
	}
	return nil
}

type _convertFileID struct {
	ID interface{} `bson:"_id"`
}

func convertFileID(fileID interface{}) (bsonx.Val, error) {
	id := _convertFileID{
		ID: fileID,
	}

	b, err := bson.Marshal(id)
	if err != nil {
		return bsonx.Val{}, err
	}
	val := bsoncore.Document(b).Lookup("_id")
	var res bsonx.Val
	err = res.UnmarshalBSONValue(val.Type, val.Data)
	return res, err
}

func deadlineContext(deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.Equal(time.Time{}) {
		return context.Background(), nil
	}

	return context.WithDeadline(context.Background(), deadline)
}
