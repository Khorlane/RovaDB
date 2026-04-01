package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const (
	walHeaderSize        = 20
	walRecordTypeSize    = 4
	walFramePayloadSize  = 24 + PageSize
	WALFrameSize         = walRecordTypeSize + walFramePayloadSize
	walCommitPayloadSize = 12
	WALCommitRecordSize  = walRecordTypeSize + walCommitPayloadSize
)

var (
	walMagic                 = [8]byte{'R', 'O', 'V', 'A', 'W', 'A', 'L', '1'}
	errUnsupportedWALVersion = errors.New("storage: unsupported wal version")
	errWALPageSizeMismatch   = errors.New("storage: wal page size mismatch")
	errUnknownWALRecordType  = errors.New("storage: unknown wal record type")
)

const (
	WALRecordTypeFrame  uint32 = 1
	WALRecordTypeCommit uint32 = 2
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

// WALCommitRecord stores one fixed-size commit-boundary marker.
type WALCommitRecord struct {
	CommitLSN uint64
	Reserved  uint32
}

// WALRecord stores one typed WAL record from the post-header stream.
type WALRecord struct {
	Type   uint32
	Frame  *WALFrame
	Commit *WALCommitRecord
}

// WALPath returns the sidecar WAL path for a database file.
func WALPath(dbPath string) string {
	return dbPath + ".wal"
}

// WriteWALHeader writes the fixed WAL header.
func WriteWALHeader(w io.Writer, h WALHeader) error {
	if h.Magic == ([8]byte{}) {
		h.Magic = walMagic
	}
	if h.WALVersion == 0 {
		h.WALVersion = CurrentWALVersion
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
	if !SupportedWALVersion(h.WALVersion) {
		return WALHeader{}, errUnsupportedWALVersion
	}

	h.DBFormatVersion = binary.LittleEndian.Uint32(raw[12:16])
	h.PageSize = binary.LittleEndian.Uint32(raw[16:20])
	if h.PageSize != PageSize {
		return WALHeader{}, errWALPageSizeMismatch
	}
	if !CompatibleWALWithDB(h.WALVersion, h.DBFormatVersion) {
		return WALHeader{}, errCorruptedWALHeader
	}

	return h, nil
}

// EnsureWALFile opens or creates a WAL sidecar with a validated header.
func EnsureWALFile(dbPath string, dbFormatVersion uint32) error {
	if !CompatibleWALWithDB(CurrentWALVersion, dbFormatVersion) {
		return errCorruptedWALHeader
	}

	path := WALPath(dbPath)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		if err := WriteWALHeader(file, WALHeader{
			Magic:           walMagic,
			WALVersion:      CurrentWALVersion,
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
	if !CompatibleWALWithDB(header.WALVersion, dbFormatVersion) || header.DBFormatVersion != dbFormatVersion {
		return errCorruptedWALHeader
	}
	return nil
}

// EncodeWALFrame encodes one fixed-width WAL frame.
func EncodeWALFrame(frame WALFrame) []byte {
	raw := make([]byte, WALFrameSize)
	binary.LittleEndian.PutUint32(raw[0:4], WALRecordTypeFrame)
	binary.LittleEndian.PutUint64(raw[4:12], frame.FrameLSN)
	binary.LittleEndian.PutUint32(raw[12:16], frame.PageID)
	binary.LittleEndian.PutUint64(raw[16:24], frame.PageLSN)
	binary.LittleEndian.PutUint32(raw[24:28], frame.Reserved)
	copy(raw[28:], frame.PageData[:])
	return raw
}

// DecodeWALFrame decodes and validates one fixed-width WAL frame.
func DecodeWALFrame(data []byte) (WALFrame, error) {
	if len(data) != WALFrameSize {
		return WALFrame{}, errCorruptedWALFrame
	}
	if binary.LittleEndian.Uint32(data[0:4]) != WALRecordTypeFrame {
		return WALFrame{}, errCorruptedWALFrame
	}

	var frame WALFrame
	frame.FrameLSN = binary.LittleEndian.Uint64(data[4:12])
	frame.PageID = binary.LittleEndian.Uint32(data[12:16])
	frame.PageLSN = binary.LittleEndian.Uint64(data[16:24])
	frame.Reserved = binary.LittleEndian.Uint32(data[24:28])
	copy(frame.PageData[:], data[28:])

	if err := ValidateWALFrame(frame); err != nil {
		return WALFrame{}, err
	}
	return frame, nil
}

// EncodeWALCommitRecord encodes one fixed-width WAL commit record.
func EncodeWALCommitRecord(rec WALCommitRecord) []byte {
	raw := make([]byte, WALCommitRecordSize)
	binary.LittleEndian.PutUint32(raw[0:4], WALRecordTypeCommit)
	binary.LittleEndian.PutUint64(raw[4:12], rec.CommitLSN)
	binary.LittleEndian.PutUint32(raw[12:16], rec.Reserved)
	return raw
}

// DecodeWALCommitRecord decodes and validates one fixed-width WAL commit record.
func DecodeWALCommitRecord(data []byte) (WALCommitRecord, error) {
	if len(data) != WALCommitRecordSize {
		return WALCommitRecord{}, errCorruptedWALFrame
	}
	if binary.LittleEndian.Uint32(data[0:4]) != WALRecordTypeCommit {
		return WALCommitRecord{}, errCorruptedWALFrame
	}

	rec := WALCommitRecord{
		CommitLSN: binary.LittleEndian.Uint64(data[4:12]),
		Reserved:  binary.LittleEndian.Uint32(data[12:16]),
	}
	if rec.CommitLSN == 0 {
		return WALCommitRecord{}, errCorruptedWALFrame
	}
	return rec, nil
}

// ValidateWALFrame validates a decoded WAL frame and its embedded page image.
func ValidateWALFrame(frame WALFrame) error {
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

// AppendWALCommitRecord appends one validated commit record after prior WAL records.
func AppendWALCommitRecord(dbPath string, rec WALCommitRecord) error {
	if rec.CommitLSN == 0 {
		return errCorruptedWALFrame
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
	_, err = file.Write(EncodeWALCommitRecord(rec))
	return err
}

// SyncWALFile fsyncs the WAL sidecar after appended records are durable-ready.
func SyncWALFile(dbPath string) error {
	file, err := os.OpenFile(WALPath(dbPath), os.O_RDWR, 0)
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
	return file.Sync()
}

// ResetWALFile rewrites the WAL sidecar to a valid header-only state.
func ResetWALFile(dbPath string, dbFormatVersion uint32) error {
	if !CompatibleWALWithDB(CurrentWALVersion, dbFormatVersion) {
		return errCorruptedWALHeader
	}

	path := WALPath(dbPath)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
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
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := WriteWALHeader(file, WALHeader{
		Magic:           walMagic,
		WALVersion:      CurrentWALVersion,
		DBFormatVersion: dbFormatVersion,
		PageSize:        PageSize,
	}); err != nil {
		return err
	}
	return file.Sync()
}

// ReadWALRecords reads and validates all typed WAL records after the header.
func ReadWALRecords(dbPath string) ([]WALRecord, error) {
	file, err := os.Open(WALPath(dbPath))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := ReadWALHeader(file); err != nil {
		return nil, err
	}

	records := make([]WALRecord, 0)
	for {
		var recordTypeRaw [walRecordTypeSize]byte
		_, err := io.ReadFull(file, recordTypeRaw[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return records, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, errCorruptedWALFrame
			}
			return nil, err
		}

		recordType := binary.LittleEndian.Uint32(recordTypeRaw[:])
		switch recordType {
		case WALRecordTypeFrame:
			raw := make([]byte, WALFrameSize)
			copy(raw[0:4], recordTypeRaw[:])
			if _, err := io.ReadFull(file, raw[4:]); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil, errCorruptedWALFrame
				}
				return nil, err
			}
			frame, err := DecodeWALFrame(raw)
			if err != nil {
				return nil, err
			}
			frameCopy := frame
			records = append(records, WALRecord{Type: WALRecordTypeFrame, Frame: &frameCopy})
		case WALRecordTypeCommit:
			raw := make([]byte, WALCommitRecordSize)
			copy(raw[0:4], recordTypeRaw[:])
			if _, err := io.ReadFull(file, raw[4:]); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return nil, errCorruptedWALFrame
				}
				return nil, err
			}
			rec, err := DecodeWALCommitRecord(raw)
			if err != nil {
				return nil, err
			}
			recCopy := rec
			records = append(records, WALRecord{Type: WALRecordTypeCommit, Commit: &recCopy})
		default:
			return nil, errUnknownWALRecordType
		}
	}
}

// ReadWALFrames reads and validates only frame records after the WAL header.
func ReadWALFrames(dbPath string) ([]WALFrame, error) {
	records, err := ReadWALRecords(dbPath)
	if err != nil {
		return nil, err
	}

	frames := make([]WALFrame, 0, len(records))
	for _, record := range records {
		if record.Type != WALRecordTypeFrame || record.Frame == nil {
			continue
		}
		frames = append(frames, *record.Frame)
	}
	return frames, nil
}

// CommittedWALFrames returns all frames belonging to fully committed WAL records.
func CommittedWALFrames(dbPath string) ([]WALFrame, error) {
	records, err := ReadWALRecords(dbPath)
	if err != nil {
		return nil, err
	}

	committed := make([]WALFrame, 0)
	currentTxn := make([]WALFrame, 0)
	for _, record := range records {
		switch record.Type {
		case WALRecordTypeFrame:
			if record.Frame == nil {
				return nil, errCorruptedWALFrame
			}
			currentTxn = append(currentTxn, *record.Frame)
		case WALRecordTypeCommit:
			if record.Commit == nil {
				return nil, errCorruptedWALFrame
			}
			committed = append(committed, currentTxn...)
			currentTxn = currentTxn[:0]
		default:
			return nil, errUnknownWALRecordType
		}
	}
	return committed, nil
}

// NextWALLSN returns the next monotonic LSN after existing WAL contents.
func NextWALLSN(dbPath string) (uint64, error) {
	records, err := ReadWALRecords(dbPath)
	if err != nil {
		return 0, err
	}

	var maxLSN uint64
	for _, record := range records {
		switch record.Type {
		case WALRecordTypeFrame:
			if record.Frame != nil && record.Frame.FrameLSN > maxLSN {
				maxLSN = record.Frame.FrameLSN
			}
		case WALRecordTypeCommit:
			if record.Commit != nil && record.Commit.CommitLSN > maxLSN {
				maxLSN = record.Commit.CommitLSN
			}
		}
	}
	if maxLSN == 0 {
		return 1, nil
	}
	return maxLSN + 1, nil
}

// ApplyWALFramesToDB overwrites main database pages with committed WAL images.
func ApplyWALFramesToDB(dbPath string, frames []WALFrame) error {
	if len(frames) == 0 {
		return nil
	}

	file, err := os.OpenFile(dbPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, frame := range frames {
		if err := ValidateWALFrame(frame); err != nil {
			return err
		}
		currentPageLSN, err := currentDBPageLSNForReplay(file, frame)
		if err != nil {
			return err
		}
		if frame.PageLSN <= currentPageLSN {
			continue
		}
		if _, err := file.WriteAt(frame.PageData[:], pageOffset(PageID(frame.PageID))); err != nil {
			return err
		}
	}
	return nil
}

func currentDBPageLSNForReplay(file *os.File, frame WALFrame) (uint64, error) {
	if file == nil {
		return 0, nil
	}

	info, err := file.Stat()
	if err != nil {
		return 0, err
	}

	offset := pageOffset(PageID(frame.PageID))
	if info.Size() < offset+PageSize {
		return 0, nil
	}

	pageData := make([]byte, PageSize)
	if _, err := file.ReadAt(pageData, offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil
		}
		return 0, err
	}

	if frame.PageID == catalogPageID {
		if bytes.Equal(pageData, frame.PageData[:]) {
			return frame.PageLSN, nil
		}
		return 0, nil
	}

	currentPageLSN, err := PageLSN(pageData)
	if err != nil {
		return 0, nil
	}
	return currentPageLSN, nil
}

func validateWALPageImage(pageID uint32, pageLSN uint64, pageData []byte) error {
	if len(pageData) != PageSize {
		return errCorruptedWALFrame
	}

	if pageID == catalogPageID {
		if binary.LittleEndian.Uint32(pageData[pageHeaderOffsetPageID:pageHeaderOffsetPageID+4]) != uint32(DirectoryControlPageID) {
			return errCorruptedWALFrame
		}
		if PageType(binary.LittleEndian.Uint16(pageData[pageHeaderOffsetPageType:pageHeaderOffsetPageType+2])) != PageTypeDirectory {
			return errCorruptedWALFrame
		}
		if err := ValidatePageImage(pageData); err != nil {
			return errCorruptedWALFrame
		}
		return nil
	}

	if embeddedPageID := binary.LittleEndian.Uint32(pageData[0:4]); embeddedPageID != pageID {
		return errCorruptedWALFrame
	}
	embeddedPageLSN, err := PageLSN(pageData)
	if err != nil || embeddedPageLSN != pageLSN {
		return errCorruptedWALFrame
	}
	if err := ValidatePageImage(pageData); err != nil {
		return errCorruptedWALFrame
	}

	return nil
}
