package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	walVersion    = 1
	walHeaderSize = 20
)

var (
	walMagic                 = [8]byte{'R', 'O', 'V', 'A', 'W', 'A', 'L', '1'}
	errUnsupportedWALVersion = errors.New("storage: unsupported wal version")
	errWALPageSizeMismatch   = errors.New("storage: wal page size mismatch")
)

// WALHeader is the fixed-width WAL file header.
type WALHeader struct {
	Magic           [8]byte
	WALVersion      uint32
	DBFormatVersion uint32
	PageSize        uint32
}

// WALPath returns the sidecar WAL path for a database file.
func WALPath(dbPath string) string {
	return dbPath + ".wal"
}

// DBFormatVersion reports the current durable database file format version.
func DBFormatVersion() uint32 {
	return version
}

// WriteWALHeader writes the fixed WAL header.
func WriteWALHeader(w io.Writer, h WALHeader) error {
	if h.Magic == ([8]byte{}) {
		h.Magic = walMagic
	}
	if h.WALVersion == 0 {
		h.WALVersion = walVersion
	}

	var raw [walHeaderSize]byte
	copy(raw[:8], h.Magic[:])
	binary.LittleEndian.PutUint32(raw[8:12], h.WALVersion)
	binary.LittleEndian.PutUint32(raw[12:16], h.DBFormatVersion)
	binary.LittleEndian.PutUint32(raw[16:20], h.PageSize)

	_, err := w.Write(raw[:])
	return err
}

// ReadWALHeader reads and validates the fixed WAL header.
func ReadWALHeader(r io.Reader) (WALHeader, error) {
	var raw [walHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return WALHeader{}, errCorruptedWALHeader
		}
		return WALHeader{}, err
	}

	var h WALHeader
	copy(h.Magic[:], raw[:8])
	if h.Magic != walMagic {
		return WALHeader{}, errCorruptedWALHeader
	}

	h.WALVersion = binary.LittleEndian.Uint32(raw[8:12])
	if h.WALVersion != walVersion {
		return WALHeader{}, errUnsupportedWALVersion
	}

	h.DBFormatVersion = binary.LittleEndian.Uint32(raw[12:16])
	h.PageSize = binary.LittleEndian.Uint32(raw[16:20])
	if h.PageSize != PageSize {
		return WALHeader{}, errWALPageSizeMismatch
	}

	return h, nil
}

// EnsureWALFile opens or creates a WAL sidecar with a validated header.
func EnsureWALFile(dbPath string, dbFormatVersion uint32) error {
	path := WALPath(dbPath)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		if err := WriteWALHeader(file, WALHeader{
			Magic:           walMagic,
			WALVersion:      walVersion,
			DBFormatVersion: dbFormatVersion,
			PageSize:        PageSize,
		}); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			_ = os.Remove(path)
			return err
		}
		return file.Close()
	} else if err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	header, err := ReadWALHeader(file)
	if err != nil {
		return err
	}
	if header.DBFormatVersion != dbFormatVersion {
		return errCorruptedWALHeader
	}
	return nil
}
