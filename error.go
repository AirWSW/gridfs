package gridfs

import "errors"

var (
	// ErrFileNotFound occurs if a user asks to download a file with a file ID that isn't found in the files collection.
	ErrFileNotFound = errors.New("file with given parameters not found")

	// ErrWrongIndex is used when the chunk retrieved from the server does not have the expected index.
	ErrWrongIndex = errors.New("chunk index does not match expected index")

	// ErrWrongSize is used when the chunk retrieved from the server does not have the expected size.
	ErrWrongSize = errors.New("chunk size does not match expected size")

	// ErrFileInvalid indicates an invalid argument.
	// Methods on File will return this error when the receiver is nil.
	ErrFileInvalid = errors.New("invalid argument")

	// ErrStreamClosed is an error returned if an operation is attempted on a closed/aborted stream.
	ErrStreamClosed = errors.New("stream is closed or aborted")

	// ErrStreamSeekUnsupport is an error returned if unsupported whence value.
	ErrStreamSeekUnsupport = errors.New("unsupported whence value")

	// ErrStreamSeekOverflow is an error returned if seek past end of file.
	ErrStreamSeekOverflow = errors.New("seek past end of file")

	// ErrStreamPermission is an error returned if an operation is attempted on a not readable/writable stream.
	ErrStreamPermission = errors.New("stream is not readable or writable")
)

var errNoMoreChunks = errors.New("no more chunks remaining")
