package storage

import (
	"bytes"
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
