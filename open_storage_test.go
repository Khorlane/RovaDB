package rovadb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenCreatesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	_ = os.Remove(path)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat(%q) error = %v", path, err)
	}
}

func TestOpenExistingValidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer db.Close()
}

func TestOpenInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	if err := os.WriteFile(path, []byte("not-a-rovadb-file"), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		db.Close()
		t.Fatal("Open() error = nil, want non-nil")
	}
}
