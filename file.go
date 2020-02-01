package gridfs

import (
	"time"

	"go.mongodb.org/mongo-driver/x/bsonx"
)

// File represents a GridFS file.
type File struct {
	ID         interface{} `bson:"_id"`
	ChunkSize  int32       `bson:"chunkSize"`
	Length     int64       `bson:"length"`
	Filename   string      `bson:"filename"`
	UploadDate time.Time   `bson:"uploadDate"`
	Metadata   bsonx.Doc   `bson:"metadata"`
}
