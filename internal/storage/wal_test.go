package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Khorlane/RovaDB/internal/parser"
)

func TestWALPath(t *testing.T) {
	got := WALPath(filepath.Join("tmp", "test.db"))
	want := filepath.Join("tmp", "test.db") + ".wal"
	if got != want {
		t.Fatalf("WALPath() = %q, want %q", got, want)
	}
}

func TestEnsureWALFileCreatesValidHeader(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	file, err := os.Open(WALPath(dbPath))
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer file.Close()

	header, err := ReadWALHeader(file)
	if err != nil {
		t.Fatalf("ReadWALHeader() error = %v", err)
	}
	if header.Magic != walMagic {
		t.Fatalf("header.Magic = %q, want %q", header.Magic, walMagic)
	}
	if header.WALVersion != walVersion {
		t.Fatalf("header.WALVersion = %d, want %d", header.WALVersion, walVersion)
	}
	if header.DBFormatVersion != DBFormatVersion() {
		t.Fatalf("header.DBFormatVersion = %d, want %d", header.DBFormatVersion, DBFormatVersion())
	}
	if header.PageSize != PageSize {
		t.Fatalf("header.PageSize = %d, want %d", header.PageSize, PageSize)
	}
}

func TestEnsureWALFileReopensExistingValidHeader(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("first EnsureWALFile() error = %v", err)
	}
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("second EnsureWALFile() error = %v", err)
	}
}

func TestReadWALHeaderRejectsBadMagic(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteWALHeader(&buf, WALHeader{
		Magic:           [8]byte{'B', 'A', 'D', 'W', 'A', 'L', '0', '0'},
		WALVersion:      walVersion,
		DBFormatVersion: DBFormatVersion(),
		PageSize:        PageSize,
	}); err != nil {
		t.Fatalf("WriteWALHeader() error = %v", err)
	}

	_, err := ReadWALHeader(&buf)
	if !errors.Is(err, errCorruptedWALHeader) {
		t.Fatalf("ReadWALHeader() error = %v, want %v", err, errCorruptedWALHeader)
	}
}

func TestReadWALHeaderRejectsTruncatedHeader(t *testing.T) {
	_, err := ReadWALHeader(bytes.NewReader([]byte("short")))
	if !errors.Is(err, errCorruptedWALHeader) {
		t.Fatalf("ReadWALHeader() error = %v, want %v", err, errCorruptedWALHeader)
	}
}

func TestReadWALHeaderRejectsWrongPageSize(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteWALHeader(&buf, WALHeader{
		Magic:           walMagic,
		WALVersion:      walVersion,
		DBFormatVersion: DBFormatVersion(),
		PageSize:        2048,
	}); err != nil {
		t.Fatalf("WriteWALHeader() error = %v", err)
	}

	_, err := ReadWALHeader(&buf)
	if !errors.Is(err, errWALPageSizeMismatch) {
		t.Fatalf("ReadWALHeader() error = %v, want %v", err, errWALPageSizeMismatch)
	}
}

func TestReadWALHeaderRejectsUnsupportedVersion(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteWALHeader(&buf, WALHeader{
		Magic:           walMagic,
		WALVersion:      walVersion + 1,
		DBFormatVersion: DBFormatVersion(),
		PageSize:        PageSize,
	}); err != nil {
		t.Fatalf("WriteWALHeader() error = %v", err)
	}

	_, err := ReadWALHeader(&buf)
	if !errors.Is(err, errUnsupportedWALVersion) {
		t.Fatalf("ReadWALHeader() error = %v, want %v", err, errUnsupportedWALVersion)
	}
}

func TestEnsureWALFileRejectsWrongDBFormatVersion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	if err := EnsureWALFile(dbPath, DBFormatVersion()+1); !errors.Is(err, errCorruptedWALHeader) {
		t.Fatalf("EnsureWALFile() error = %v, want %v", err, errCorruptedWALHeader)
	}
}

func TestWALFrameRoundTrip(t *testing.T) {
	frame := buildTestWALFrame(t, 7, 11, 13)

	got, err := DecodeWALFrame(EncodeWALFrame(frame))
	if err != nil {
		t.Fatalf("DecodeWALFrame() error = %v", err)
	}
	if got.FrameLSN != frame.FrameLSN || got.PageID != frame.PageID || got.PageLSN != frame.PageLSN || got.Reserved != frame.Reserved {
		t.Fatalf("round-trip frame header = %#v, want %#v", got, frame)
	}
	if got.PageData != frame.PageData {
		t.Fatal("round-trip page data mismatch")
	}
}

func TestWALCommitRecordRoundTrip(t *testing.T) {
	want := WALCommitRecord{CommitLSN: 11, Reserved: 7}

	got, err := DecodeWALCommitRecord(EncodeWALCommitRecord(want))
	if err != nil {
		t.Fatalf("DecodeWALCommitRecord() error = %v", err)
	}
	if got != want {
		t.Fatalf("round-trip commit record = %#v, want %#v", got, want)
	}
}

func TestAppendWALFrameWritesFrameAfterHeader(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	frame := buildTestWALFrame(t, 3, 5, 0)
	if err := AppendWALFrame(dbPath, frame); err != nil {
		t.Fatalf("AppendWALFrame() error = %v", err)
	}

	info, err := os.Stat(WALPath(dbPath))
	if err != nil {
		t.Fatalf("os.Stat() error = %v", err)
	}
	if got, want := info.Size(), int64(walHeaderSize+WALFrameSize); got != want {
		t.Fatalf("wal size = %d, want %d", got, want)
	}

	raw, err := os.ReadFile(WALPath(dbPath))
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	if len(raw) != walHeaderSize+WALFrameSize {
		t.Fatalf("len(raw) = %d, want %d", len(raw), walHeaderSize+WALFrameSize)
	}
	if _, err := ReadWALHeader(bytes.NewReader(raw[:walHeaderSize])); err != nil {
		t.Fatalf("ReadWALHeader(raw header) error = %v", err)
	}
	got, err := DecodeWALFrame(raw[walHeaderSize:])
	if err != nil {
		t.Fatalf("DecodeWALFrame(raw frame) error = %v", err)
	}
	if got.FrameLSN != frame.FrameLSN || got.PageID != frame.PageID || got.PageLSN != frame.PageLSN {
		t.Fatalf("decoded file frame = %#v, want %#v", got, frame)
	}
	if recordType := binary.LittleEndian.Uint32(raw[walHeaderSize : walHeaderSize+4]); recordType != WALRecordTypeFrame {
		t.Fatalf("record type = %d, want %d", recordType, WALRecordTypeFrame)
	}
}

func TestReadWALRecordsReturnsMixedRecordsInOrder(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	want1 := buildTestWALFrame(t, 2, 10, 0)
	want2 := buildTestWALFrame(t, 4, 11, 0)
	wantCommit := WALCommitRecord{CommitLSN: 11, Reserved: 3}
	if err := AppendWALFrame(dbPath, want1); err != nil {
		t.Fatalf("AppendWALFrame(first) error = %v", err)
	}
	if err := AppendWALFrame(dbPath, want2); err != nil {
		t.Fatalf("AppendWALFrame(second) error = %v", err)
	}
	if err := AppendWALCommitRecord(dbPath, wantCommit); err != nil {
		t.Fatalf("AppendWALCommitRecord() error = %v", err)
	}

	got, err := ReadWALRecords(dbPath)
	if err != nil {
		t.Fatalf("ReadWALRecords() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len(ReadWALRecords()) = %d, want 3", len(got))
	}
	if got[0].Type != WALRecordTypeFrame || got[0].Frame == nil {
		t.Fatalf("first record = %#v, want frame", got[0])
	}
	if got[0].Frame.FrameLSN != want1.FrameLSN || got[0].Frame.PageID != want1.PageID || got[0].Frame.PageLSN != want1.PageLSN {
		t.Fatalf("first frame = %#v, want %#v", got[0].Frame, want1)
	}
	if got[1].Type != WALRecordTypeFrame || got[1].Frame == nil {
		t.Fatalf("second record = %#v, want frame", got[1])
	}
	if got[1].Frame.FrameLSN != want2.FrameLSN || got[1].Frame.PageID != want2.PageID || got[1].Frame.PageLSN != want2.PageLSN {
		t.Fatalf("second frame = %#v, want %#v", got[1].Frame, want2)
	}
	if got[2].Type != WALRecordTypeCommit || got[2].Commit == nil {
		t.Fatalf("third record = %#v, want commit", got[2])
	}
	if *got[2].Commit != wantCommit {
		t.Fatalf("commit record = %#v, want %#v", *got[2].Commit, wantCommit)
	}
}

func TestReadWALFramesRejectsTruncatedFrame(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	file, err := os.OpenFile(WALPath(dbPath), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	raw := make([]byte, WALFrameSize-1)
	binary.LittleEndian.PutUint32(raw[0:4], WALRecordTypeFrame)
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		t.Fatalf("file.Write() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	_, err = ReadWALFrames(dbPath)
	if !errors.Is(err, errCorruptedWALFrame) {
		t.Fatalf("ReadWALFrames() error = %v, want %v", err, errCorruptedWALFrame)
	}
}

func TestReadWALRecordsRejectsTruncatedCommitRecord(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	file, err := os.OpenFile(WALPath(dbPath), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	raw := make([]byte, WALCommitRecordSize-1)
	binary.LittleEndian.PutUint32(raw[0:4], WALRecordTypeCommit)
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		t.Fatalf("file.Write() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	_, err = ReadWALRecords(dbPath)
	if !errors.Is(err, errCorruptedWALFrame) {
		t.Fatalf("ReadWALRecords() error = %v, want %v", err, errCorruptedWALFrame)
	}
}

func TestDecodeWALFrameRejectsBadChecksum(t *testing.T) {
	frame := buildTestWALFrame(t, 5, 9, 0)
	raw := EncodeWALFrame(frame)
	raw[walRecordTypeSize+24+100] ^= 0xFF

	_, err := DecodeWALFrame(raw)
	if !errors.Is(err, errCorruptedWALFrame) {
		t.Fatalf("DecodeWALFrame() error = %v, want %v", err, errCorruptedWALFrame)
	}
}

func TestDecodeWALFrameRejectsZeroPageID(t *testing.T) {
	frame := buildTestWALFrame(t, 1, 2, 0)
	frame.PageID = 0
	raw := EncodeWALFrame(frame)

	_, err := DecodeWALFrame(raw)
	if !errors.Is(err, errCorruptedWALFrame) {
		t.Fatalf("DecodeWALFrame() error = %v, want %v", err, errCorruptedWALFrame)
	}
}

func TestReadWALFramesRejectsCorruptedStoredFrame(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	frame := buildTestWALFrame(t, 8, 12, 0)
	if err := AppendWALFrame(dbPath, frame); err != nil {
		t.Fatalf("AppendWALFrame() error = %v", err)
	}

	file, err := os.OpenFile(WALPath(dbPath), os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	offset := int64(walHeaderSize + walRecordTypeSize + 24 + 25)
	if _, err := file.WriteAt([]byte{0x7F}, offset); err != nil {
		_ = file.Close()
		t.Fatalf("file.WriteAt() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	_, err = ReadWALFrames(dbPath)
	if !errors.Is(err, errCorruptedWALFrame) {
		t.Fatalf("ReadWALFrames() error = %v, want %v", err, errCorruptedWALFrame)
	}
}

func TestReadWALRecordsRejectsUnknownRecordType(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	file, err := os.OpenFile(WALPath(dbPath), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], 99)
	if _, err := file.Write(raw[:]); err != nil {
		_ = file.Close()
		t.Fatalf("file.Write() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("file.Close() error = %v", err)
	}

	_, err = ReadWALRecords(dbPath)
	if !errors.Is(err, errUnknownWALRecordType) {
		t.Fatalf("ReadWALRecords() error = %v, want %v", err, errUnknownWALRecordType)
	}
}

func TestAppendWALCommitRecordWritesAfterFrames(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	frame1 := buildTestWALFrame(t, 2, 10, 0)
	frame2 := buildTestWALFrame(t, 3, 11, 0)
	commit := WALCommitRecord{CommitLSN: 11, Reserved: 1}
	if err := AppendWALFrame(dbPath, frame1); err != nil {
		t.Fatalf("AppendWALFrame(first) error = %v", err)
	}
	if err := AppendWALFrame(dbPath, frame2); err != nil {
		t.Fatalf("AppendWALFrame(second) error = %v", err)
	}
	if err := AppendWALCommitRecord(dbPath, commit); err != nil {
		t.Fatalf("AppendWALCommitRecord() error = %v", err)
	}

	raw, err := os.ReadFile(WALPath(dbPath))
	if err != nil {
		t.Fatalf("os.ReadFile() error = %v", err)
	}
	wantSize := walHeaderSize + 2*WALFrameSize + WALCommitRecordSize
	if len(raw) != wantSize {
		t.Fatalf("len(raw) = %d, want %d", len(raw), wantSize)
	}
	commitOffset := walHeaderSize + 2*WALFrameSize
	if recordType := binary.LittleEndian.Uint32(raw[commitOffset : commitOffset+4]); recordType != WALRecordTypeCommit {
		t.Fatalf("commit record type = %d, want %d", recordType, WALRecordTypeCommit)
	}
	gotCommit, err := DecodeWALCommitRecord(raw[commitOffset : commitOffset+WALCommitRecordSize])
	if err != nil {
		t.Fatalf("DecodeWALCommitRecord(raw commit) error = %v", err)
	}
	if gotCommit != commit {
		t.Fatalf("decoded commit = %#v, want %#v", gotCommit, commit)
	}
}

func buildTestWALFrame(t *testing.T, pageID uint32, frameLSN uint64, reserved uint32) WALFrame {
	t.Helper()

	page := InitializeTablePage(pageID)
	binary.LittleEndian.PutUint64(page[tablePageHeaderOffsetPageLSN:tablePageHeaderOffsetPageLSN+8], frameLSN)
	row, err := EncodeSlottedRow([]parser.Value{parser.StringValue("row")})
	if err != nil {
		t.Fatalf("EncodeSlottedRow() error = %v", err)
	}
	if _, err := InsertRowIntoTablePage(page, row); err != nil {
		t.Fatalf("InsertRowIntoTablePage() error = %v", err)
	}
	if err := RecomputePageChecksum(page); err != nil {
		t.Fatalf("RecomputePageChecksum() error = %v", err)
	}

	var frame WALFrame
	frame.FrameLSN = frameLSN
	frame.PageID = pageID
	frame.PageLSN = frameLSN
	frame.Reserved = reserved
	copy(frame.PageData[:], page)
	return frame
}
