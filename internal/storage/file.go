package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	version = 1
)

var magic = [8]byte{'R', 'O', 'V', 'A', 'D', 'B', 0, 0}

// DBFile is the minimal durable database file handle.
type DBFile struct {
	path string
	file *os.File
}

// File returns the underlying file handle for internal wiring.
func (f *DBFile) File() *os.File {
	if f == nil {
		return nil
	}
	return f.file
}

// OpenOrCreate opens an existing database file or creates a new one with a header.
func OpenOrCreate(path string) (*DBFile, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return nil, err
		}
		if err := writeHeader(file); err != nil {
			file.Close()
			_ = os.Remove(path)
			return nil, err
		}
		return &DBFile{path: path, file: file}, nil
	} else if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := readHeader(file); err != nil {
		file.Close()
		return nil, err
	}

	return &DBFile{path: path, file: file}, nil
}

// Close releases the underlying database file.
func (f *DBFile) Close() error {
	if f == nil || f.file == nil {
		return nil
	}
	return f.file.Close()
}

func writeHeader(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var header [HeaderSize]byte
	copy(header[:8], magic[:])
	binary.LittleEndian.PutUint32(header[8:12], version)

	if _, err := f.Write(header[:]); err != nil {
		return err
	}
	return f.Sync()
}

func readHeader(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var header [HeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return errCorruptedDatabaseHeader
	}
	if string(header[:8]) != string(magic[:]) {
		return errCorruptedDatabaseHeader
	}
	if binary.LittleEndian.Uint32(header[8:12]) != version {
		return errCorruptedDatabaseHeader
	}
	for _, b := range header[12:16] {
		if b != 0 {
			return errCorruptedDatabaseHeader
		}
	}

	return nil
}
