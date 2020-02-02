package gridfs

import (
	"time"

	"go.mongodb.org/mongo-driver/mongo"
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

// File represents an open file descriptor.
type File struct {
	name        string
	nonblock    bool // whether we set nonblocking mode
	stdoutOrErr bool // whether this is stdout or stderr
	appendMode  bool // whether file is opened for appending

	*stream  // gridfs file stream specific
	fileInfo *FileInfo
}

// FileInfo represents an database file descriptor.
type FileInfo struct {
	ID         interface{} `bson:"_id"`
	ChunkSize  int32       `bson:"chunkSize"`
	Filename   string      `bson:"filename"`
	Length     int64       `bson:"length"`
	Metadata   bsonx.Doc   `bson:"metadata"`
	Reference  bsonx.Doc   `bson:"reference"`
	UploadDate time.Time   `bson:"uploadDate"`
}

const (
	// DefaultBucketName is the default name of bucket.
	DefaultBucketName string = "fs"
	// DefaultChunkSize is the default size of each file chunk.
	DefaultChunkSize int32 = 255 * 1024 // 255KiB
	// DefaultReadBufferSize is the default size of each file chunk.
	DefaultReadBufferSize int = 255 * 1024 // 255KiB
	// DefaultWriteBufferSize is the default size of each file chunk.
	DefaultWriteBufferSize int = 4 * 1024 * 1024 // 4MiB
)
