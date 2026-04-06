package rovadb

import (
	"errors"
	"os"
	"testing"

	"github.com/Khorlane/RovaDB/internal/storage"
)

func TestOpenRejectsOrphanWALWhenDBFileIsMissing(t *testing.T) {
	path := testDBPath(t)
	walPath := storage.WALPath(path)

	if err := storage.EnsureWALFile(path, storage.DBFormatVersion()); err != nil {
		t.Fatalf("EnsureWALFile() error = %v", err)
	}

	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open() error = nil, want orphan WAL failure")
	}
	if err.Error() != "open: database file does not exist but WAL sidecar exists: "+walPath {
		t.Fatalf("Open() error = %q, want %q", err.Error(), "open: database file does not exist but WAL sidecar exists: "+walPath)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("os.Stat(db path) error = %v, want %v", statErr, os.ErrNotExist)
	}
}
