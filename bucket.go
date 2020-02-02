package gridfs

import (
	"bytes"
	"context"
	"os"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// Bucket represents a GridFS bucket.
type Bucket struct {
	name      string // The bucket name. Defaults to "fs".
	chunkSize int32  // The bucket chunk size in bytes. Defaults to 255KiB.
	created   bool   // The bucket created. Defaults to false.

	db *mongo.Database            // The database for the bucket.
	wc *writeconcern.WriteConcern // The write concern for the bucket. Defaults to the write concern of the database.
	rc *readconcern.ReadConcern   // The read concern for the bucket. Defaults to the read concern of the database.
	rp *readpref.ReadPref         // The read preference for the bucket. Defaults to the read preference of the database.

	chunksColl *mongo.Collection // The collection to store file chunks.
	filesColl  *mongo.Collection // The collection to store file metadata.
	tempsColl  *mongo.Collection // The collection to store file temps.

	openDeadline  time.Time
	readDeadline  time.Time
	writeDeadline time.Time
}

// SetOpenDeadline sets the open deadline for this bucket
func (b *Bucket) SetOpenDeadline(t time.Time) error {
	b.openDeadline = t
	return nil
}

// SetReadDeadline sets the read deadline for this bucket
func (b *Bucket) SetReadDeadline(t time.Time) error {
	b.readDeadline = t
	return nil
}

// SetWriteDeadline sets the write deadline for this bucket.
func (b *Bucket) SetWriteDeadline(t time.Time) error {
	b.writeDeadline = t
	return nil
}

// newFile returns a new File with the given file handle and name.
func (b *Bucket) newFile(fileInfo *FileInfo, flag int) *File {
	f := &File{&stream{
		fileInfo:   fileInfo,
		chunksColl: b.chunksColl,
		filesColl:  b.filesColl,
		tempsColl:  b.tempsColl,
	}}
	runtime.SetFinalizer(f.stream, (*stream).close)

	// Ignore initialization errors.
	// Assume any problems will show up in later I/O.
	f.stream.init(StreamRead | StreamWrite)

	return f
}

func (b *Bucket) openFile(name string, flag int, perm os.FileMode) (file *File, err error) {
	panic("v")
}

// openFileNolog is the Unix implementation of OpenFile.
func (b *Bucket) openFileNolog(name string, flag int, perm os.FileMode) (*File, error) {
	panic("v")
}

func (b *Bucket) openByID(ctx context.Context, fileID interface{}) (*mongo.Cursor, error) {
	id, err := convertFileID(fileID)
	if err != nil {
		return nil, err
	}
	filter := bsonx.Doc{bsonx.Elem{Key: "_id", Value: id}}
	return b.findFile(ctx, filter)
}

func (b *Bucket) openByFilename(ctx context.Context, filename string, revision *int32) (*mongo.Cursor, error) {
	var numSkip int32 = -1
	var sortOrder int32 = 1

	if revision != nil {
		numSkip = *revision
	}

	if numSkip < 0 {
		sortOrder = -1
		numSkip = (-1 * numSkip) - 1
	}

	filter := bsonx.Doc{bsonx.Elem{Key: "filename", Value: bsonx.String(filename)}}
	findOpts := options.Find().SetSkip(int64(numSkip)).SetSort(bsonx.Doc{bsonx.Elem{Key: "uploadDate", Value: bsonx.Int32(sortOrder)}})

	return b.findFile(ctx, filter, findOpts)
}

func (b *Bucket) findFile(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	cursor, err := b.filesColl.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	if !cursor.Next(ctx) {
		_ = cursor.Close(ctx)
		return nil, ErrFileNotFound
	}
	return cursor, nil
}

func (b *Bucket) deleteChunks(ctx context.Context, fileID interface{}) error {
	id, err := convertFileID(fileID)
	if err != nil {
		return err
	}
	_, err = b.chunksColl.DeleteMany(ctx, bsonx.Doc{bsonx.Elem{Key: "files_id", Value: id}})
	if err != nil {
		_, _ = b.tempsColl.DeleteMany(ctx, bsonx.Doc{bsonx.Elem{Key: "files_id", Value: id}})
		return err
	}
	_, err = b.tempsColl.DeleteMany(ctx, bsonx.Doc{bsonx.Elem{Key: "files_id", Value: id}})
	return err
}

func (b *Bucket) checkIndexesCreated(ctx context.Context) error {
	if !b.created {
		// before the first write operation, must determine if files collection is empty
		// if so, create indexes if they do not already exist
		if err := b.createIndexes(ctx); err != nil {
			return err
		}
		b.created = true
	}
	return nil
}

// create indexes on the files and chunks collection if needed
func (b *Bucket) createIndexes(ctx context.Context) error {
	// must use primary read pref mode to check if files coll empty
	cloned, err := b.filesColl.Clone(options.Collection().SetReadPreference(readpref.Primary()))
	if err != nil {
		return err
	}

	docRes := cloned.FindOne(ctx, bsonx.Doc{}, options.FindOne().SetProjection(bsonx.Doc{bsonx.Elem{Key: "_id", Value: bsonx.Int32(1)}}))

	_, err = docRes.DecodeBytes()
	if err != mongo.ErrNoDocuments {
		// nil, or error that occured during the FindOne operation
		return err
	}

	chunksIv := b.chunksColl.Indexes()
	tempsIv := b.tempsColl.Indexes()
	filesIv := b.filesColl.Indexes()

	chunksModel := mongo.IndexModel{
		Keys: bson.D{
			primitive.E{Key: "files_id", Value: int32(1)},
			primitive.E{Key: "n", Value: int32(1)},
		},
		Options: options.Index().SetUnique(true),
	}
	tempsModel := mongo.IndexModel{
		Keys: bson.D{
			primitive.E{Key: "files_id", Value: int32(1)},
			primitive.E{Key: "n", Value: int32(1)},
			primitive.E{Key: "from", Value: int32(1)},
			primitive.E{Key: "to", Value: int32(1)},
			primitive.E{Key: "length", Value: int32(1)},
		},
		Options: options.Index().SetUnique(true),
	}
	filesModel := mongo.IndexModel{
		Keys: bson.D{
			primitive.E{Key: "filename", Value: int32(1)},
			primitive.E{Key: "uploadDate", Value: int32(1)},
		},
	}

	if err = createIndexIfNotExists(ctx, chunksIv, chunksModel); err != nil {
		return err
	}
	if err = createIndexIfNotExists(ctx, tempsIv, tempsModel); err != nil {
		return err
	}
	if err = createIndexIfNotExists(ctx, filesIv, filesModel); err != nil {
		return err
	}

	return nil
}

// Create an index if it doesn't already exist
func createIndexIfNotExists(ctx context.Context, iv mongo.IndexView, model mongo.IndexModel) error {
	c, err := iv.List(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = c.Close(ctx)
	}()

	var found bool
	for c.Next(ctx) {
		keyElem, err := c.Current.LookupErr("key")
		if err != nil {
			return err
		}

		keyElemDoc := keyElem.Document()
		modelKeysDoc, err := bson.Marshal(model.Keys)
		if err != nil {
			return err
		}

		if bytes.Equal(modelKeysDoc, keyElemDoc) {
			found = true
			break
		}
	}

	if !found {
		_, err = iv.CreateOne(ctx, model)
		if err != nil {
			return err
		}
	}

	return nil
}
