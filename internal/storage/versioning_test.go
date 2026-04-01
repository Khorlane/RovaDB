package storage

import (
	"errors"
	"testing"
)

func TestSupportedDBFormatVersionAcceptsCurrentVersion(t *testing.T) {
	if !SupportedDBFormatVersion(CurrentDBFormatVersion) {
		t.Fatalf("SupportedDBFormatVersion(%d) = false, want true", CurrentDBFormatVersion)
	}
}

func TestSupportedWALVersionAcceptsCurrentVersion(t *testing.T) {
	if !SupportedWALVersion(CurrentWALVersion) {
		t.Fatalf("SupportedWALVersion(%d) = false, want true", CurrentWALVersion)
	}
}

func TestSupportedDBFormatVersionRejectsUnsupportedVersion(t *testing.T) {
	if SupportedDBFormatVersion(CurrentDBFormatVersion + 1) {
		t.Fatalf("SupportedDBFormatVersion(%d) = true, want false", CurrentDBFormatVersion+1)
	}
}

func TestSupportedWALVersionRejectsUnsupportedVersion(t *testing.T) {
	if SupportedWALVersion(CurrentWALVersion + 1) {
		t.Fatalf("SupportedWALVersion(%d) = true, want false", CurrentWALVersion+1)
	}
}

func TestCompatibleWALWithDBRequiresMatchingSupportedVersions(t *testing.T) {
	if !CompatibleWALWithDB(CurrentWALVersion, CurrentDBFormatVersion) {
		t.Fatal("CompatibleWALWithDB(current,current) = false, want true")
	}
	if CompatibleWALWithDB(CurrentWALVersion+1, CurrentDBFormatVersion) {
		t.Fatal("CompatibleWALWithDB(unsupported wal,current db) = true, want false")
	}
	if CompatibleWALWithDB(CurrentWALVersion, CurrentDBFormatVersion+1) {
		t.Fatal("CompatibleWALWithDB(current wal,unsupported db) = true, want false")
	}
}

func TestValidateFormatSignatureAcceptsCurrentMatchingSignature(t *testing.T) {
	err := ValidateFormatSignature(FormatSignature{
		DBFormatVersion:        CurrentDBFormatVersion,
		DirectoryFormatVersion: CurrentDBFormatVersion,
		WALVersion:             CurrentWALVersion,
		WALDBFormatVersion:     CurrentDBFormatVersion,
		PageSize:               PageSize,
	})
	if err != nil {
		t.Fatalf("ValidateFormatSignature() error = %v", err)
	}
}

func TestValidateFormatSignatureRejectsDBDirectoryMismatch(t *testing.T) {
	err := ValidateFormatSignature(FormatSignature{
		DBFormatVersion:        CurrentDBFormatVersion,
		DirectoryFormatVersion: CurrentDBFormatVersion + 1,
		WALVersion:             CurrentWALVersion,
		WALDBFormatVersion:     CurrentDBFormatVersion,
		PageSize:               PageSize,
	})
	if !errors.Is(err, errCorruptedDirectoryPage) {
		t.Fatalf("ValidateFormatSignature() error = %v, want %v", err, errCorruptedDirectoryPage)
	}
}

func TestValidateFormatSignatureRejectsWALDBFormatMismatch(t *testing.T) {
	err := ValidateFormatSignature(FormatSignature{
		DBFormatVersion:        CurrentDBFormatVersion,
		DirectoryFormatVersion: CurrentDBFormatVersion,
		WALVersion:             CurrentWALVersion,
		WALDBFormatVersion:     CurrentDBFormatVersion + 1,
		PageSize:               PageSize,
	})
	if !errors.Is(err, errCorruptedWALHeader) {
		t.Fatalf("ValidateFormatSignature() error = %v, want %v", err, errCorruptedWALHeader)
	}
}

func TestValidateFormatSignatureRejectsUnsupportedDBFormat(t *testing.T) {
	err := ValidateFormatSignature(FormatSignature{
		DBFormatVersion:        CurrentDBFormatVersion + 1,
		DirectoryFormatVersion: CurrentDBFormatVersion + 1,
		WALVersion:             CurrentWALVersion,
		WALDBFormatVersion:     CurrentDBFormatVersion + 1,
		PageSize:               PageSize,
	})
	if !errors.Is(err, errCorruptedDatabaseHeader) {
		t.Fatalf("ValidateFormatSignature() error = %v, want %v", err, errCorruptedDatabaseHeader)
	}
}

func TestValidateFormatSignatureRejectsUnsupportedWALVersion(t *testing.T) {
	err := ValidateFormatSignature(FormatSignature{
		DBFormatVersion:        CurrentDBFormatVersion,
		DirectoryFormatVersion: CurrentDBFormatVersion,
		WALVersion:             CurrentWALVersion + 1,
		WALDBFormatVersion:     CurrentDBFormatVersion,
		PageSize:               PageSize,
	})
	if !errors.Is(err, errUnsupportedWALVersion) {
		t.Fatalf("ValidateFormatSignature() error = %v, want %v", err, errUnsupportedWALVersion)
	}
}
