package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
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
	if header.WALVersion != CurrentWALVersion {
		t.Fatalf("header.WALVersion = %d, want %d", header.WALVersion, CurrentWALVersion)
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
		WALVersion:      CurrentWALVersion,
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
		WALVersion:      CurrentWALVersion,
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
		WALVersion:      CurrentWALVersion + 1,
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

func TestReadWALHeaderRejectsUnsupportedDBFormatVersion(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteWALHeader(&buf, WALHeader{
		Magic:           walMagic,
		WALVersion:      CurrentWALVersion,
		DBFormatVersion: CurrentDBFormatVersion + 1,
		PageSize:        PageSize,
	}); err != nil {
		t.Fatalf("WriteWALHeader() error = %v", err)
	}

	_, err := ReadWALHeader(&buf)
	if !errors.Is(err, errCorruptedWALHeader) {
		t.Fatalf("ReadWALHeader() error = %v, want %v", err, errCorruptedWALHeader)
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

func TestEnsureWALFileRejectsUnsupportedDBFormatVersionOnCreate(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	if err := EnsureWALFile(dbPath, CurrentDBFormatVersion+1); !errors.Is(err, errCorruptedWALHeader) {
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

func TestSyncWALFileSucceedsOnValidWAL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	frame := buildTestWALFrame(t, 2, 10, 0)
	if err := AppendWALFrame(dbPath, frame); err != nil {
		t.Fatalf("AppendWALFrame() error = %v", err)
	}
	if err := AppendWALCommitRecord(dbPath, WALCommitRecord{CommitLSN: 11}); err != nil {
		t.Fatalf("AppendWALCommitRecord() error = %v", err)
	}

	if err := SyncWALFile(dbPath); err != nil {
		t.Fatalf("SyncWALFile() error = %v", err)
	}
}

func TestSyncWALFileFailsOnMissingWAL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "missing.db")

	if err := SyncWALFile(dbPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("SyncWALFile() error = %v, want %v", err, os.ErrNotExist)
	}
}

func TestResetWALFileLeavesHeaderOnlyWAL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}
	if err := AppendWALFrame(dbPath, buildTestWALFrame(t, 2, 10, 0)); err != nil {
		t.Fatalf("AppendWALFrame() error = %v", err)
	}
	if err := AppendWALCommitRecord(dbPath, WALCommitRecord{CommitLSN: 11}); err != nil {
		t.Fatalf("AppendWALCommitRecord() error = %v", err)
	}

	if err := ResetWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("ResetWALFile() error = %v", err)
	}

	info, err := os.Stat(WALPath(dbPath))
	if err != nil {
		t.Fatalf("os.Stat() error = %v", err)
	}
	if got, want := info.Size(), int64(walHeaderSize); got != want {
		t.Fatalf("wal size = %d, want %d", got, want)
	}
	records, err := ReadWALRecords(dbPath)
	if err != nil {
		t.Fatalf("ReadWALRecords() error = %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("len(ReadWALRecords()) = %d, want 0", len(records))
	}
}

func TestApplyWALFramesToDBWritesMissingPage(t *testing.T) {
	dbPath := createReplayTestDB(t)
	frame := buildTestWALFrameWithValue(t, 3, 1, "missing")

	if err := ApplyWALFramesToDB(dbPath, []WALFrame{frame}); err != nil {
		t.Fatalf("ApplyWALFramesToDB() error = %v", err)
	}

	got := readReplayTestDBPage(t, dbPath, frame.PageID)
	if !bytes.Equal(got, frame.PageData[:]) {
		t.Fatal("replayed page bytes do not match WAL frame for missing page")
	}
}

func TestApplyWALFramesToDBWritesNewerPageLSN(t *testing.T) {
	dbPath := createReplayTestDB(t)
	current := buildTestWALFrameWithValue(t, 2, 5, "older")
	newer := buildTestWALFrameWithValue(t, 2, 7, "newer")
	writeReplayTestDBPage(t, dbPath, current.PageID, current.PageData[:])

	if err := ApplyWALFramesToDB(dbPath, []WALFrame{newer}); err != nil {
		t.Fatalf("ApplyWALFramesToDB() error = %v", err)
	}

	got := readReplayTestDBPage(t, dbPath, newer.PageID)
	if !bytes.Equal(got, newer.PageData[:]) {
		t.Fatal("replayed page bytes do not match newer WAL frame")
	}
}

func TestApplyWALFramesToDBSkipsEqualPageLSN(t *testing.T) {
	dbPath := createReplayTestDB(t)
	current := buildTestWALFrameWithValue(t, 2, 7, "current")
	equal := buildTestWALFrameWithValue(t, 2, 7, "equal")
	writeReplayTestDBPage(t, dbPath, current.PageID, current.PageData[:])

	if err := ApplyWALFramesToDB(dbPath, []WALFrame{equal}); err != nil {
		t.Fatalf("ApplyWALFramesToDB() error = %v", err)
	}

	got := readReplayTestDBPage(t, dbPath, current.PageID)
	if !bytes.Equal(got, current.PageData[:]) {
		t.Fatal("equal PageLSN frame should have been skipped")
	}
}

func TestApplyWALFramesToDBSkipsOlderPageLSN(t *testing.T) {
	dbPath := createReplayTestDB(t)
	current := buildTestWALFrameWithValue(t, 2, 9, "current")
	older := buildTestWALFrameWithValue(t, 2, 7, "older")
	writeReplayTestDBPage(t, dbPath, current.PageID, current.PageData[:])

	if err := ApplyWALFramesToDB(dbPath, []WALFrame{older}); err != nil {
		t.Fatalf("ApplyWALFramesToDB() error = %v", err)
	}

	got := readReplayTestDBPage(t, dbPath, current.PageID)
	if !bytes.Equal(got, current.PageData[:]) {
		t.Fatal("older PageLSN frame should have been skipped")
	}
}

func TestApplyWALFramesToDBIsIdempotent(t *testing.T) {
	dbPath := createReplayTestDB(t)
	frame := buildTestWALFrameWithValue(t, 4, 11, "idempotent")

	if err := ApplyWALFramesToDB(dbPath, []WALFrame{frame}); err != nil {
		t.Fatalf("first ApplyWALFramesToDB() error = %v", err)
	}
	first := readReplayTestDBPage(t, dbPath, frame.PageID)

	if err := ApplyWALFramesToDB(dbPath, []WALFrame{frame}); err != nil {
		t.Fatalf("second ApplyWALFramesToDB() error = %v", err)
	}
	second := readReplayTestDBPage(t, dbPath, frame.PageID)

	if !bytes.Equal(first, second) {
		t.Fatal("replay is not idempotent across repeated ApplyWALFramesToDB() calls")
	}
}

func TestApplyWALFramesToDBAppliesAndSkipsPerPage(t *testing.T) {
	dbPath := createReplayTestDB(t)
	page2Current := buildTestWALFrameWithValue(t, 2, 8, "page2-current")
	page3Newer := buildTestWALFrameWithValue(t, 3, 4, "page3-newer")
	page3Current := buildTestWALFrameWithValue(t, 3, 2, "page3-current")
	page4Frame := buildTestWALFrameWithValue(t, 4, 1, "page4-new")
	writeReplayTestDBPage(t, dbPath, page2Current.PageID, page2Current.PageData[:])
	writeReplayTestDBPage(t, dbPath, page3Current.PageID, page3Current.PageData[:])

	page2Older := buildTestWALFrameWithValue(t, 2, 7, "page2-older")
	if err := ApplyWALFramesToDB(dbPath, []WALFrame{page2Older, page3Newer, page4Frame}); err != nil {
		t.Fatalf("ApplyWALFramesToDB() error = %v", err)
	}

	gotPage2 := readReplayTestDBPage(t, dbPath, page2Current.PageID)
	if !bytes.Equal(gotPage2, page2Current.PageData[:]) {
		t.Fatal("stale page 2 frame should have been skipped")
	}

	gotPage3 := readReplayTestDBPage(t, dbPath, page3Newer.PageID)
	if !bytes.Equal(gotPage3, page3Newer.PageData[:]) {
		t.Fatal("newer page 3 frame should have been applied")
	}

	gotPage4 := readReplayTestDBPage(t, dbPath, page4Frame.PageID)
	if !bytes.Equal(gotPage4, page4Frame.PageData[:]) {
		t.Fatal("missing page 4 frame should have been applied")
	}
}

func TestNextWALLSNReturnsNextMonotonicValue(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	if err := EnsureWALFile(dbPath, DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}
	if err := AppendWALFrame(dbPath, buildTestWALFrame(t, 2, 10, 0)); err != nil {
		t.Fatalf("AppendWALFrame() error = %v", err)
	}
	if err := AppendWALCommitRecord(dbPath, WALCommitRecord{CommitLSN: 11}); err != nil {
		t.Fatalf("AppendWALCommitRecord() error = %v", err)
	}

	nextLSN, err := NextWALLSN(dbPath)
	if err != nil {
		t.Fatalf("NextWALLSN() error = %v", err)
	}
	if nextLSN != 12 {
		t.Fatalf("NextWALLSN() = %d, want 12", nextLSN)
	}
}

func buildTestWALFrame(t *testing.T, pageID uint32, frameLSN uint64, reserved uint32) WALFrame {
	t.Helper()

	page := InitializeTablePage(pageID)
	binary.LittleEndian.PutUint64(page[tablePageHeaderOffsetPageLSN:tablePageHeaderOffsetPageLSN+8], frameLSN)
	row, err := EncodeSlottedRow([]Value{StringValue("row")}, []uint8{CatalogColumnTypeText})
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

func buildTestWALFrameWithValue(t *testing.T, pageID uint32, frameLSN uint64, value string) WALFrame {
	t.Helper()

	page := InitializeTablePage(pageID)
	if err := SetPageLSN(page, frameLSN); err != nil {
		t.Fatalf("SetPageLSN() error = %v", err)
	}
	row, err := EncodeSlottedRow([]Value{StringValue(value)}, []uint8{CatalogColumnTypeText})
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
	copy(frame.PageData[:], page)
	return frame
}

func createReplayTestDB(t *testing.T) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "replay.db")
	dbFile, err := OpenOrCreate(dbPath)
	if err != nil {
		t.Fatalf("OpenOrCreate() error = %v", err)
	}
	if err := dbFile.Close(); err != nil {
		t.Fatalf("dbFile.Close() error = %v", err)
	}
	return dbPath
}

func writeReplayTestDBPage(t *testing.T, dbPath string, pageID uint32, pageData []byte) {
	t.Helper()

	file, err := os.OpenFile(dbPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("os.OpenFile() error = %v", err)
	}
	defer file.Close()

	if _, err := file.WriteAt(pageData, pageOffset(PageID(pageID))); err != nil {
		t.Fatalf("file.WriteAt() error = %v", err)
	}
}

func readReplayTestDBPage(t *testing.T, dbPath string, pageID uint32) []byte {
	t.Helper()

	file, err := os.Open(dbPath)
	if err != nil {
		t.Fatalf("os.Open() error = %v", err)
	}
	defer file.Close()

	pageData := make([]byte, PageSize)
	if _, err := file.ReadAt(pageData, pageOffset(PageID(pageID))); err != nil {
		t.Fatalf("file.ReadAt() error = %v", err)
	}
	return pageData
}
