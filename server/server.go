package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/AirWSW/gridfs"
)

var bucket *gridfs.Bucket

func downloadHandler(c *gin.Context) {
	filename := c.Param("filename")
	log.Println(c.Request.Header)
	ds, err := bucket.OpenDownloadStreamByName(filename)
	if err != nil {
		http.NotFound(c.Writer, c.Request)
		return
	}
	c.Writer.Header().Set("Content-Disposition", fmt.Sprintf("inline; filename=%s", filename))
	http.ServeContent(c.Writer, c.Request, filename, time.Now(), ds)
}

func main() {
	clientOpts := options.Client().ApplyURI("mongodb://admin:admin@localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOpts)
	if err != nil {
		log.Fatal(err)
	}
	db := client.Database("gridfs")
	defer func() {
		_ = client.Disconnect(context.Background())
	}()
	bucket, err = gridfs.NewBucket(
		db,
		options.GridFSBucket().SetName("test_fs"),
	)
	if err != nil {
		log.Fatal(err)
	}
	_, _ = bucket.OpenUploadStream("")
	r := gin.Default()
	r.GET("/download/:filename", downloadHandler)
	log.Fatal(r.Run(":8080"))
}
