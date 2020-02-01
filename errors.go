package gridfs

import "errors"

// ErrFileNotFound occurs if a user asks to download a file with a file ID that isn't found in the files collection.
var ErrFileNotFound = errors.New("file with given parameters not found")

// ErrWrongIndex is used when the chunk retrieved from the server does not have the expected index.
var ErrWrongIndex = errors.New("chunk index does not match expected index")

// ErrWrongSize is used when the chunk retrieved from the server does not have the expected size.
var ErrWrongSize = errors.New("chunk size does not match expected size")

// ErrStreamClosed is an error returned if an operation is attempted on a closed/aborted stream.
var ErrStreamClosed = errors.New("stream is closed or aborted")

var errNoMoreChunks = errors.New("no more chunks remaining")
