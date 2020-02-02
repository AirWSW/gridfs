package old

import (
	"errors"
	"io"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
)

func newFile(fileInfo *FileInfo, chunksColl, filesColl, tempsColl *mongo.Collection) *File {
	return &File{
		name:     fileInfo.Filename,
		stream:   newStream(fileInfo, chunksColl, filesColl, tempsColl),
		fileInfo: fileInfo,
	}
}

// Name returns the name of the file as presented to Open.
func (f *File) Name() string { return f.name }

// Close closes the File, rendering it unusable for I/O.
// On files that support SetDeadline, any pending I/O operations will
// be canceled and return immediately with an error.
// Close will return an error if it has already been called.
func (f *File) Close() (err error) {
	if f == nil {
		err = os.ErrInvalid
	}
	if e := f.stream.close(); e != nil {
		err = &os.PathError{Op: "close", Path: f.name, Err: e}
	}
	return
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and any error encountered.
// At end of file, Read returns 0, io.EOF.
func (f *File) Read(b []byte) (n int, err error) {
	if err := f.checkValid("read"); err != nil {
		return 0, err
	}
	n, e := f.stream.read(b)
	return n, f.wrapErr("read", e)
}

// ReadAt reads len(b) bytes from the File starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	if err := f.checkValid("read"); err != nil {
		return 0, err
	}

	if off < 0 {
		return 0, &os.PathError{Op: "readat", Path: f.name, Err: errors.New("negative offset")}
	}

	for len(b) > 0 {
		m, e := f.pread(b, off)
		if e != nil {
			err = f.wrapErr("read", e)
			break
		}
		n += m
		b = b[m:]
		off += int64(m)
	}
	return
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *File) Write(b []byte) (n int, err error) {
	if err := f.checkValid("write"); err != nil {
		return 0, err
	}
	n, e := f.stream.write(b)
	if n < 0 {
		n = 0
	}
	if n != len(b) {
		err = io.ErrShortWrite
	}

	// epipecheck(f, e)

	if e != nil {
		err = f.wrapErr("write", e)
	}

	return n, err
}

var errWriteAtInAppendMode = errors.New("os: invalid use of WriteAt on file opened with O_APPEND")

// WriteAt writes len(b) bytes to the File starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(b).
//
// If file was opened with the O_APPEND flag, WriteAt returns an error.
func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	if err := f.checkValid("write"); err != nil {
		return 0, err
	}
	if f.appendMode {
		return 0, errWriteAtInAppendMode
	}

	if off < 0 {
		return 0, &os.PathError{Op: "writeat", Path: f.name, Err: errors.New("negative offset")}
	}

	for len(b) > 0 {
		m, e := f.pwrite(b, off)
		if e != nil {
			err = f.wrapErr("write", e)
			break
		}
		n += m
		b = b[m:]
		off += int64(m)
	}
	return
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
// The behavior of Seek on a file opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (ret int64, err error) {
	if err := f.checkValid("seek"); err != nil {
		return 0, err
	}
	r, e := f.stream.seek(offset, whence)
	if e != nil {
		return 0, f.wrapErr("seek", e)
	}
	return r, nil
}

// If not, it returns an appropriate error, perhaps incorporating the operation name op.
func (f *File) checkValid(op string) error {
	if f == nil {
		return os.ErrInvalid
	}
	return nil
}

// wrapErr wraps an error that occurred during an operation on an open file.
// It passes io.EOF through unchanged, otherwise converts
// poll.ErrFileClosing to ErrClosed and wraps the error in a PathError.
func (f *File) wrapErr(op string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	// if err == poll.ErrFileClosing {
	// 	err = os.ErrClosed
	// }
	return &os.PathError{Op: op, Path: f.name, Err: err}
}

func (i *FileInfo) Revision() *int32 {
	return nil
}
