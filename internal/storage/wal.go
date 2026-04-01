package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	walVersion         = 1
	walHeaderSize      = 20
	walFrameHeaderSize = 24
	WALFrameSize       = walFrameHeaderSize + PageSize
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

// WALFrame stores one full-page WAL image.
type WALFrame struct {
	FrameLSN uint64
	PageID   uint32
	PageLSN  uint64
	Reserved uint32
	PageData [PageSize]byte
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

// EncodeWALFrame encodes one fixed-width WAL frame.
func EncodeWALFrame(frame WALFrame) []byte {
	raw := make([]byte, WALFrameSize)
	binary.LittleEndian.PutUint64(raw[0:8], frame.FrameLSN)
	binary.LittleEndian.PutUint32(raw[8:12], frame.PageID)
	binary.LittleEndian.PutUint64(raw[12:20], frame.PageLSN)
	binary.LittleEndian.PutUint32(raw[20:24], frame.Reserved)
	copy(raw[walFrameHeaderSize:], frame.PageData[:])
	return raw
}

// DecodeWALFrame decodes and validates one fixed-width WAL frame.
func DecodeWALFrame(data []byte) (WALFrame, error) {
	if len(data) != WALFrameSize {
		return WALFrame{}, errCorruptedWALFrame
	}

	var frame WALFrame
	frame.FrameLSN = binary.LittleEndian.Uint64(data[0:8])
	frame.PageID = binary.LittleEndian.Uint32(data[8:12])
	frame.PageLSN = binary.LittleEndian.Uint64(data[12:20])
	frame.Reserved = binary.LittleEndian.Uint32(data[20:24])
	copy(frame.PageData[:], data[walFrameHeaderSize:])

	if err := ValidateWALFrame(frame); err != nil {
		return WALFrame{}, err
	}
	return frame, nil
}

// ValidateWALFrame validates a decoded WAL frame and its embedded page image.
func ValidateWALFrame(frame WALFrame) error {
	if frame.PageID == 0 {
		return errCorruptedWALFrame
	}
	if err := validateWALPageImage(frame.PageID, frame.PageLSN, frame.PageData[:]); err != nil {
		return err
	}
	return nil
}

// AppendWALFrame appends one validated frame after the WAL header.
func AppendWALFrame(dbPath string, frame WALFrame) error {
	if err := ValidateWALFrame(frame); err != nil {
		return err
	}

	path := WALPath(dbPath)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := ReadWALHeader(file); err != nil {
		return err
	}
	_, err = file.Write(EncodeWALFrame(frame))
	return err
}

// ReadWALFrames reads and validates all frames after the WAL header.
func ReadWALFrames(dbPath string) ([]WALFrame, error) {
	file, err := os.Open(WALPath(dbPath))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := ReadWALHeader(file); err != nil {
		return nil, err
	}

	frames := make([]WALFrame, 0)
	for {
		raw := make([]byte, WALFrameSize)
		_, err := io.ReadFull(file, raw)
		if err == nil {
			frame, err := DecodeWALFrame(raw)
			if err != nil {
				return nil, err
			}
			frames = append(frames, frame)
			continue
		}
		if errors.Is(err, io.EOF) {
			return frames, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errCorruptedWALFrame
		}
		return nil, err
	}
}

func validateWALPageImage(pageID uint32, pageLSN uint64, pageData []byte) error {
	if len(pageData) != PageSize {
		return errCorruptedWALFrame
	}

	if embeddedPageID := binary.LittleEndian.Uint32(pageData[0:4]); embeddedPageID != pageID {
		return errCorruptedWALFrame
	}
	if embeddedPageLSN := binary.LittleEndian.Uint64(pageData[8:16]); embeddedPageLSN != pageLSN {
		return errCorruptedWALFrame
	}

	storedChecksum := binary.LittleEndian.Uint32(pageData[16:20])
	if storedChecksum != walPageChecksum(pageData) {
		return errCorruptedWALFrame
	}

	pageType := PageType(binary.LittleEndian.Uint16(pageData[4:6]))
	switch pageType {
	case PageTypeTable:
		if err := validateSlottedTablePage(pageData); err != nil {
			return errCorruptedWALFrame
		}
	case PageTypeIndexLeaf, PageTypeIndexInternal:
		if err := validateIndexPage(pageData); err != nil {
			return errCorruptedWALFrame
		}
	default:
		return errCorruptedWALFrame
	}

	return nil
}

func walPageChecksum(pageData []byte) uint32 {
	var checksum uint32
	for i, b := range pageData {
		if i >= 16 && i < 20 {
			continue
		}
		checksum = checksum*16777619 ^ uint32(b)
	}
	return checksum
}

func setWALPageChecksum(pageData []byte) {
	if len(pageData) != PageSize {
		return
	}
	binary.LittleEndian.PutUint32(pageData[16:20], 0)
	binary.LittleEndian.PutUint32(pageData[16:20], walPageChecksum(pageData))
}
