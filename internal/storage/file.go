package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
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

// Open opens an existing database file and validates its durable header.
func Open(path string) (*DBFile, error) {
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

// Create creates a new database file and initializes its durable header.
func Create(path string) (*DBFile, error) {
	if _, err := os.Stat(path); err == nil {
		return nil, os.ErrExist
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

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
	binary.LittleEndian.PutUint32(header[8:12], CurrentDBFormatVersion)

	if _, err := f.Write(header[:]); err != nil {
		return err
	}
	return f.Sync()
}

func readHeader(f *os.File) error {
	_, err := ReadDBFormatVersion(f)
	return err
}

// ReadDBFormatVersion reads and validates the durable DB header version.
func ReadDBFormatVersion(f *os.File) (uint32, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var header [HeaderSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		return 0, errCorruptedDatabaseHeader
	}
	if string(header[:8]) != string(magic[:]) {
		return 0, errCorruptedDatabaseHeader
	}
	formatVersion := binary.LittleEndian.Uint32(header[8:12])
	if !SupportedDBFormatVersion(formatVersion) {
		return 0, errCorruptedDatabaseHeader
	}
	for _, b := range header[12:16] {
		if b != 0 {
			return 0, errCorruptedDatabaseHeader
		}
	}

	return formatVersion, nil
}
