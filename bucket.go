package gridfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

// Create creates or truncates the named file. If the file already exists,
// it is truncated. If the file does not exist, it is created with mode 0666
// (before umask). If successful, methods on the returned File can
// be used for I/O; the associated file descriptor has mode O_RDWR.
// If there is an error, it will be of type *PathError.
func (b *Bucket) Create(name string, fileInfo *FileInfo) (*File, error) {
	if fileInfo != nil && fileInfo.Filename != name {
		return nil, errors.New("text")
	} else if fileInfo == nil {
		fileInfo = &FileInfo{
			ID:       primitive.NewObjectID(),
			Filename: name,
		}
	}
	return b.OpenFile(fileInfo, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// CreateWithID creates or truncates the named file. If the file already exists,
// it is truncated. If the file does not exist, it is created with mode 0666
// (before umask). If successful, methods on the returned File can
// be used for I/O; the associated file descriptor has mode O_RDWR.
// If there is an error, it will be of type *PathError.
func (b *Bucket) CreateWithID(name string, fileID interface{}, fileInfo *FileInfo) (*File, error) {
	if fileInfo != nil && (fileInfo.ID != fileID || fileInfo.Filename != name) {
		return nil, errors.New("text")
	} else if fileInfo == nil {
		fileInfo = &FileInfo{
			ID:       fileID,
			Filename: name,
		}
	}
	return b.OpenFile(fileInfo, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// Open opens the named file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func (b *Bucket) Open(fileID interface{}, fileInfo *FileInfo) (*File, error) {
	if fileInfo != nil && fileInfo.ID != fileID {
		return nil, errors.New("text")
	} else if fileInfo == nil {
		fileInfo = &FileInfo{
			ID: fileID,
		}
	}
	return b.OpenFile(fileInfo, os.O_RDONLY, 0)
}

// OpenByName opens the named file for reading. If successful, methods on
// the returned file can be used for reading; the associated file
// descriptor has mode O_RDONLY.
// If there is an error, it will be of type *PathError.
func (b *Bucket) OpenByName(name string, fileInfo *FileInfo) (*File, error) {
	if fileInfo != nil && fileInfo.Filename != name {
		return nil, errors.New("text")
	} else if fileInfo == nil {
		fileInfo = &FileInfo{
			Filename: name,
		}
	}
	return b.OpenFile(fileInfo, os.O_RDONLY, 0)
}

// OpenFile is the generalized open call; most users will use Open
// or Create instead. It opens the named file with specified flag
// (O_RDONLY etc.). If the file does not exist, and the O_CREATE flag
// is passed, it is created with mode perm (before umask). If successful,
// methods on the returned File can be used for I/O.
// If there is an error, it will be of type *PathError.
func (b *Bucket) OpenFile(fileInfo *FileInfo, flag int, perm os.FileMode) (*File, error) {
	ctx, cancel := deadlineContext(b.openDeadline)
	if cancel != nil {
		defer cancel()
	}

	if err := b.checkIndexesCreated(ctx); err != nil {
		return nil, err
	}

	f, err := b.openFile(ctx, fileInfo, flag, perm)
	if err != nil {
		return nil, err
	}

	f.appendMode = flag&os.O_APPEND != 0

	return f, nil
}

func (b *Bucket) Hash() {
	// f.stream.closed = false
	// _, err = f.Seek(0, io.SeekStart)
	// if err != nil {
	// 	return
	// }
	// f.stream.numChunks = int32(math.Ceil(float64(f.stream.fileInfo.Length) / float64(f.stream.fileInfo.ChunkSize)))
	// r := bufio.NewReader(f)
	// h := md5.New()
	// if _, err := io.Copy(h, r); err != nil {
	// 	log.Println(err)
	// }
	// log.Printf("%x %s", h.Sum(nil), f.name)
	// f.stream.closed = true
}

// Pipe returns a connected pair of Files; reads from r return bytes written to w.
// It returns the files and an error, if any.
func (b *Bucket) Pipe() (r *File, w *File, err error) {
	panic("not implemented")
}

// Remove removes the named file or directory.
// If there is an error, it will be of type *PathError.
func (b *Bucket) Remove(fileID interface{}) error {
	return b.Delete(fileID)
}

// Rename renames oldpath to newpath.
// If newpath already exists and is not a directory, Rename replaces it.
// OS-specific restrictions may apply when oldpath and newpath are in different directories.
// If there is an error, it will be of type *LinkError.
func (b *Bucket) Rename(fileID interface{}, newFilename string) error {
	ctx, cancel := deadlineContext(b.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	id, err := convertFileID(fileID)
	if err != nil {
		return err
	}
	res, err := b.filesColl.UpdateOne(ctx,
		bsonx.Doc{bsonx.Elem{Key: "_id", Value: id}},
		bsonx.Doc{bsonx.Elem{Key: "$set", Value: bsonx.Document(bsonx.Doc{bsonx.Elem{Key: "filename", Value: bsonx.String(newFilename)}})}},
	)
	if err != nil {
		return err
	}

	if res.MatchedCount == 0 {
		return ErrFileNotFound
	}

	return nil
}

// Delete deletes all chunks and metadata associated with the file with the given file ID.
func (b *Bucket) Delete(fileID interface{}) error {
	ctx, cancel := deadlineContext(b.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	id, err := convertFileID(fileID)
	if err != nil {
		return err
	}
	res, err := b.filesColl.DeleteOne(ctx, bsonx.Doc{bsonx.Elem{Key: "_id", Value: id}})
	if err == nil && res.DeletedCount == 0 {
		err = ErrFileNotFound
	}
	if err != nil {
		_ = b.deleteChunks(ctx, fileID) // can attempt to delete chunks even if no docs in files collection matched
		return err
	}

	return b.deleteChunks(ctx, fileID)
}

// Drop drops the files and chunks collections associated with this bucket.
func (b *Bucket) Drop() error {
	ctx, cancel := deadlineContext(b.writeDeadline)
	if cancel != nil {
		defer cancel()
	}

	if err := b.filesColl.Drop(ctx); err != nil {
		return err
	}

	if err := b.tempsColl.Drop(ctx); err != nil {
		return err
	}

	return b.chunksColl.Drop(ctx)
}

// Find returns the files collection documents that match the given filter.
func (b *Bucket) Find(filter interface{}, opts ...*options.GridFSFindOptions) (*mongo.Cursor, error) {
	ctx, cancel := deadlineContext(b.readDeadline)
	if cancel != nil {
		defer cancel()
	}

	gfsOpts := options.MergeGridFSFindOptions(opts...)
	find := options.Find()
	if gfsOpts.BatchSize != nil {
		find.SetBatchSize(*gfsOpts.BatchSize)
	}
	if gfsOpts.Limit != nil {
		find.SetLimit(int64(*gfsOpts.Limit))
	}
	if gfsOpts.MaxTime != nil {
		find.SetMaxTime(*gfsOpts.MaxTime)
	}
	if gfsOpts.NoCursorTimeout != nil {
		find.SetNoCursorTimeout(*gfsOpts.NoCursorTimeout)
	}
	if gfsOpts.Skip != nil {
		find.SetSkip(int64(*gfsOpts.Skip))
	}
	if gfsOpts.Sort != nil {
		find.SetSort(gfsOpts.Sort)
	}

	return b.filesColl.Find(ctx, filter, find)
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

func (b *Bucket) openFile(ctx context.Context, fileInfo *FileInfo, flag int, perm os.FileMode) (*File, error) {
	if flag&(os.O_WRONLY|os.O_RDWR) != 0 {
		_, err := b.filesColl.InsertOne(ctx, fileInfo)
		if err != nil {
			return nil, err
		}
	} else {
		if fileInfo.ID != nil {
			cursor, err := b.openByID(ctx, fileInfo.ID)
			if err != nil {
				return nil, err
			}
			if err := bson.Unmarshal(cursor.Current, &fileInfo); err != nil {
				return nil, err
			}
		} else if fileInfo.Filename != "" {
			cursor, err := b.openByFilename(ctx, fileInfo.Filename, fileInfo.Revision())
			if err != nil {
				return nil, err
			}
			if err := bson.Unmarshal(cursor.Current, &fileInfo); err != nil {
				return nil, err
			}
		}
	}
	return newFile(fileInfo, b.chunksColl, b.filesColl, b.tempsColl), nil
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
