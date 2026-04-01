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
}

func TestReadWALFramesReturnsFramesInOrder(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	want1 := buildTestWALFrame(t, 2, 10, 0)
	want2 := buildTestWALFrame(t, 4, 20, 0)
	if err := AppendWALFrame(dbPath, want1); err != nil {
		t.Fatalf("AppendWALFrame(first) error = %v", err)
	}
	if err := AppendWALFrame(dbPath, want2); err != nil {
		t.Fatalf("AppendWALFrame(second) error = %v", err)
	}

	got, err := ReadWALFrames(dbPath)
	if err != nil {
		t.Fatalf("ReadWALFrames() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len(ReadWALFrames()) = %d, want 2", len(got))
	}
	if got[0].FrameLSN != want1.FrameLSN || got[0].PageID != want1.PageID || got[0].PageLSN != want1.PageLSN {
		t.Fatalf("first frame = %#v, want %#v", got[0], want1)
	}
	if got[1].FrameLSN != want2.FrameLSN || got[1].PageID != want2.PageID || got[1].PageLSN != want2.PageLSN {
		t.Fatalf("second frame = %#v, want %#v", got[1], want2)
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
	if _, err := file.Write(make([]byte, WALFrameSize-1)); err != nil {
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

func TestDecodeWALFrameRejectsBadChecksum(t *testing.T) {
	frame := buildTestWALFrame(t, 5, 9, 0)
	raw := EncodeWALFrame(frame)
	raw[walFrameHeaderSize+100] ^= 0xFF

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
	offset := int64(walHeaderSize + walFrameHeaderSize + 25)
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
	setWALPageChecksum(page)

	var frame WALFrame
	frame.FrameLSN = frameLSN
	frame.PageID = pageID
	frame.PageLSN = frameLSN
	frame.Reserved = reserved
	copy(frame.PageData[:], page)
	return frame
}
