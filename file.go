package gridfs

import (
	"bufio"
	"crypto/md5"
	"errors"
	"io"
	"log"
	"math"
	"os"
	"time"
)

// File represents an open file descriptor.
type File struct {
	*stream // gridfs file stream specific
}

// FileInfo represents an database file descriptor.
type FileInfo struct {
	nonblock    bool // whether we set nonblocking mode
	stdoutOrErr bool // whether this is stdout or stderr
	appendMode  bool // whether file is opened for appending

	ID         interface{}    `bson:"_id"`
	ChunkSize  int32          `bson:"chunkSize"`
	Filename   string         `bson:"filename"`
	Length     int64          `bson:"length"`
	Metadata   interface{}    `bson:"metadata"`
	Reference  *FileReference `bson:"reference,omitempty"`
	UploadDate time.Time      `bson:"uploadDate"`
}

// FileReference represents an database file reference.
type FileReference struct {
	Hashes               Hashes    `bson:"hashes,omitempty"`
	MimeType             string    `bson:"mimeType,omitempty"`
	CreatedDateTime      time.Time `bson:"createdDateTime,omitempty"`
	LastAccessedDateTime time.Time `bson:"lastAccessedDateTime,omitempty"`
	LastModifiedDateTime time.Time `bson:"lastModifiedDateTime,omitempty"`
}

// Hashes represents an database file reference.
type Hashes struct {
	MD5Hash      *string `bson:"md5Hash,omitempty"`      // hex
	CRC32Hash    *string `bson:"crc32Hash,omitempty"`    // hex
	SHA1Hash     *string `bson:"sha1Hash,omitempty"`     // hex
	QuickXorHash *string `bson:"quickXorHash,omitempty"` // base64
}

func (f *File) MD5Hash() (md5Hash *string, err error) {
	if err := f.checkValid("MD5Hash"); err != nil {
		return nil, err
	}
	if f.stream.fileInfo.Reference != nil {
		md5Hash = f.stream.fileInfo.Reference.Hashes.MD5Hash
	} else {
		f.stream.fileInfo.Reference = &FileReference{Hashes: Hashes{}}
	}
	if md5Hash == nil {
		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			return
		}
		f.stream.numChunks = int32(math.Ceil(float64(f.stream.fileInfo.Length) / float64(f.stream.fileInfo.ChunkSize)))
		r := bufio.NewReader(f)
		h := md5.New()
		if _, err := io.Copy(h, r); err != nil {
			log.Println(err)
		}
		hSum := string(h.Sum(nil))
		f.stream.fileInfo.Reference.Hashes.MD5Hash = &hSum
		log.Printf("%x %s", md5Hash, f.fileInfo.Filename)
	}
	if e := f.stream.close(); e != nil {
		err = f.wrapErr("close", e)
	}
	return
}

// Close closes the File, rendering it unusable for I/O.
// On files that support SetDeadline, any pending I/O operations will
// be canceled and return immediately with an error.
// Close will return an error if it has already been called.
func (f *File) Close() (err error) {
	if err := f.checkValid("Close"); err != nil {
		return err
	}
	ctx, cancel := deadlineContext(f.stream.writeDeadline)
	if cancel != nil {
		defer cancel()
	}
	if f.stream.flag&StreamModified != 0 {
		err := f.stream.writeChunks(ctx, true)
		if err != nil {
			return err
		}
		f.stream.flag = f.stream.flag &^ StreamModified
	}
	if err := f.stream.createFilesCollDoc(ctx); err != nil {
		return err
	}
	if e := f.stream.close(); e != nil {
		err = f.wrapErr("close", e)
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
	n, e := f.read(b)
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
		return 0, f.wrapErr("readat", errors.New("negative offset"))
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
	n, e := f.write(b)
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
	// if f.appendMode {
	// 	return 0, errWriteAtInAppendMode
	// }

	if off < 0 {
		return 0, f.wrapErr("writeat", errors.New("negative offset"))
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
	r, e := f.seek(offset, whence)
	if e != nil {
		return 0, f.wrapErr("seek", e)
	}
	return r, nil
}

// WriteString is like Write, but writes the contents of string s rather than
// a slice of bytes.
func (f *File) WriteString(s string) (n int, err error) {
	return f.Write([]byte(s))
}

func (f *File) readdir(n int) (fi []FileInfo, err error) {
	panic("v")
}

// setDeadline sets the read and write deadline.
func (f *File) setDeadline(t time.Time) error {
	if err := f.checkValid("SetDeadline"); err != nil {
		return err
	}
	return f.stream.setDeadline(t)
}

// setReadDeadline sets the read deadline.
func (f *File) setReadDeadline(t time.Time) error {
	if err := f.checkValid("SetReadDeadline"); err != nil {
		return err
	}
	return f.stream.setReadDeadline(t)
}

// setWriteDeadline sets the write deadline.
func (f *File) setWriteDeadline(t time.Time) error {
	if err := f.checkValid("SetWriteDeadline"); err != nil {
		return err
	}
	return f.stream.setWriteDeadline(t)
}

// checkValid checks whether f is valid for use.
// If not, it returns an appropriate error, perhaps incorporating the operation name op.
func (f *File) checkValid(op string) error {
	if f == nil {
		return ErrFileInvalid
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
	return &os.PathError{Op: op, Path: f.stream.fileInfo.Filename, Err: err}
}
