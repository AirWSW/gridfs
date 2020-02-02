package old

import (
	"io"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// NewBucket creates a GridFS bucket.
func NewBucket(db *mongo.Database, opts ...*options.BucketOptions) *Bucket {
	b := &Bucket{
		name:      DefaultBucketName,
		chunkSize: DefaultChunkSize,
		db:        db,
		wc:        db.WriteConcern(),
		rc:        db.ReadConcern(),
		rp:        db.ReadPreference(),
	}

	bo := options.MergeBucketOptions(opts...)
	if bo.Name != nil {
		b.name = *bo.Name
	}
	if bo.ChunkSizeBytes != nil {
		b.chunkSize = *bo.ChunkSizeBytes
	}
	if bo.WriteConcern != nil {
		b.wc = bo.WriteConcern
	}
	if bo.ReadConcern != nil {
		b.rc = bo.ReadConcern
	}
	if bo.ReadPreference != nil {
		b.rp = bo.ReadPreference
	}

	var collOpts = options.Collection().SetWriteConcern(b.wc).SetReadConcern(b.rc).SetReadPreference(b.rp)

	b.chunksColl = db.Collection(b.name+".chunks", collOpts)
	b.filesColl = db.Collection(b.name+".files", collOpts)
	b.tempsColl = db.Collection(b.name+".temps", collOpts)

	return b
}

// OpenUploadStream creates a file ID new upload stream for a file given the filename.
func (b *Bucket) OpenUploadStream(filename string, opts ...*options.UploadOptions) (*File, error) {
	return b.OpenUploadStreamWithID(primitive.NewObjectID(), filename, opts...)
}

// OpenUploadStreamWithID creates a new upload stream for a file given the file ID and filename.
func (b *Bucket) OpenUploadStreamWithID(fileID interface{}, filename string, opts ...*options.UploadOptions) (*File, error) {
	fileInfo, err := b.parseUploadOptions(fileID, filename, opts...)
	if err != nil {
		return nil, err
	}
	return b.CreateWithID(filename, fileID, fileInfo)
}

// UploadFromStream creates a fileID and uploads a file given a source stream.
func (b *Bucket) UploadFromStream(filename string, source io.Reader, opts ...*options.UploadOptions) (primitive.ObjectID, error) {
	fileID := primitive.NewObjectID()
	return fileID, b.UploadFromStreamWithID(fileID, filename, source, opts...)
}

// UploadFromStreamWithID uploads a file given a source stream.
func (b *Bucket) UploadFromStreamWithID(fileID interface{}, filename string, source io.Reader, opts ...*options.UploadOptions) error {
	us, err := b.OpenUploadStreamWithID(fileID, filename, opts...)
	if err != nil {
		return err
	}
	return b.uploadFromStream(us, source)
}

// OpenDownloadStream creates a stream from which the contents of the file can be read and seek.
func (b *Bucket) OpenDownloadStream(fileID interface{}) (*File, error) {
	ctx, cancel := deadlineContext(b.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	cursor, err := b.openByID(ctx, fileID)
	if err != nil {
		return nil, err
	}
	var fileInfo FileInfo
	if err := bson.Unmarshal(cursor.Current, &fileInfo); err != nil {
		return nil, err
	}

	return newFile(&fileInfo, b.chunksColl, b.filesColl, b.tempsColl), nil
}

// OpenDownloadStreamByName opens a download stream for the file with the given filename.
func (b *Bucket) OpenDownloadStreamByName(filename string, opts ...*options.NameOptions) (*File, error) {
	ctx, cancel := deadlineContext(b.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	cursor, err := b.openByFilename(ctx, filename, options.MergeNameOptions(opts...).Revision)
	if err != nil {
		return nil, err
	}
	var fileInfo FileInfo
	if err := bson.Unmarshal(cursor.Current, &fileInfo); err != nil {
		return nil, err
	}

	return newFile(&fileInfo, b.chunksColl, b.filesColl, b.tempsColl), nil
}

// DownloadToStream downloads the file with the specified fileID and writes it to the provided io.Writer.
// Returns the number of bytes written to the steam and an error, or nil if there was no error.
func (b *Bucket) DownloadToStream(fileID interface{}, stream io.Writer) (int64, error) {
	ds, err := b.OpenDownloadStream(fileID)
	if err != nil {
		return 0, err
	}
	return b.downloadToStream(ds, stream)
}

// DownloadToStreamByName downloads the file with the given name to the given io.Writer.
func (b *Bucket) DownloadToStreamByName(filename string, stream io.Writer, opts ...*options.NameOptions) (int64, error) {
	ds, err := b.OpenDownloadStreamByName(filename, opts...)
	if err != nil {
		return 0, err
	}
	return b.downloadToStream(ds, stream)
}

func (b *Bucket) parseUploadOptions(fileID interface{}, filename string, opts ...*options.UploadOptions) (*FileInfo, error) {
	fileInfo := &FileInfo{
		ID:        fileID,
		Filename:  filename,
		ChunkSize: b.chunkSize, // upload chunk size defaults to bucket's value
	}

	uo := options.MergeUploadOptions(opts...)
	if uo.ChunkSizeBytes != nil {
		fileInfo.ChunkSize = *uo.ChunkSizeBytes
	}
	if uo.Registry == nil {
		uo.Registry = bson.DefaultRegistry
	}
	if uo.Metadata != nil {
		raw, err := bson.MarshalWithRegistry(uo.Registry, uo.Metadata)
		if err != nil {
			return nil, err
		}
		doc, err := bsonx.ReadDoc(raw)
		if err != nil {
			return nil, err
		}
		fileInfo.Metadata = doc
	}

	return fileInfo, nil
}

func (b *Bucket) uploadFromStream(f *File, r io.Reader) error {
	if err := f.setWriteDeadline(b.writeDeadline); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (b *Bucket) downloadToStream(f *File, w io.Writer) (int64, error) {
	if err := f.setReadDeadline(b.readDeadline); err != nil {
		_ = f.Close()
		return 0, err
	}
	copied, err := io.Copy(w, f)
	if err != nil {
		_ = f.Close()
		return 0, err
	}
	return copied, f.Close()
}
