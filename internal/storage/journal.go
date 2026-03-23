package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
)

const (
	journalVersion    = 1
	journalHeaderSize = 24
)

var (
	journalMagic           = [8]byte{'R', 'O', 'V', 'A', 'J', 'N', 'L', '1'}
	errInvalidJournal      = errors.New("storage: invalid journal")
	errInvalidJournalVer   = errors.New("storage: unsupported journal version")
	errJournalSizeMismatch = errors.New("storage: journal page size mismatch")
)

// JournalHeader is the fixed-width rollback-journal header.
type JournalHeader struct {
	Magic      [8]byte
	Version    uint32
	PageSize   uint32
	EntryCount uint32
}

// JournalEntry stores one full original durable page image for later recovery.
type JournalEntry struct {
	PageID uint32
	Data   []byte
}

// Journal is a tiny DTO for rollback-journal write and recovery plumbing.
type Journal struct {
	Header  JournalHeader
	Entries []JournalEntry
}

// JournalPath returns the sidecar rollback-journal path for a database file.
func JournalPath(dbPath string) string {
	return dbPath + ".journal"
}

// WriteJournalHeader writes the fixed rollback-journal header.
func WriteJournalHeader(w io.Writer, h JournalHeader) error {
	if h.Magic == ([8]byte{}) {
		h.Magic = journalMagic
	}
	if h.Version == 0 {
		h.Version = journalVersion
	}

	var raw [journalHeaderSize]byte
	copy(raw[:8], h.Magic[:])
	binary.LittleEndian.PutUint32(raw[8:12], h.Version)
	binary.LittleEndian.PutUint32(raw[12:16], h.PageSize)
	binary.LittleEndian.PutUint32(raw[16:20], h.EntryCount)

	_, err := w.Write(raw[:])
	return err
}

// ReadJournalHeader reads and validates the fixed rollback-journal header.
func ReadJournalHeader(r io.Reader) (JournalHeader, error) {
	var raw [journalHeaderSize]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return JournalHeader{}, err
	}

	var h JournalHeader
	copy(h.Magic[:], raw[:8])
	if h.Magic != journalMagic {
		return JournalHeader{}, errInvalidJournal
	}

	h.Version = binary.LittleEndian.Uint32(raw[8:12])
	if h.Version != journalVersion {
		return JournalHeader{}, errInvalidJournalVer
	}
	h.PageSize = binary.LittleEndian.Uint32(raw[12:16])
	h.EntryCount = binary.LittleEndian.Uint32(raw[16:20])
	return h, nil
}

// WriteJournalEntry writes one fixed-width rollback-journal entry.
func WriteJournalEntry(w io.Writer, pageID uint32, data []byte) error {
	if len(data) == 0 {
		return errInvalidJournal
	}

	var raw [4]byte
	binary.LittleEndian.PutUint32(raw[:], pageID)
	if _, err := w.Write(raw[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// ReadJournalEntry reads one rollback-journal entry using the provided page size.
func ReadJournalEntry(r io.Reader, pageSize uint32) (JournalEntry, error) {
	if pageSize == 0 {
		return JournalEntry{}, errInvalidJournal
	}

	var raw [4]byte
	if _, err := io.ReadFull(r, raw[:]); err != nil {
		return JournalEntry{}, err
	}

	entry := JournalEntry{
		PageID: binary.LittleEndian.Uint32(raw[:]),
		Data:   make([]byte, pageSize),
	}
	if _, err := io.ReadFull(r, entry.Data); err != nil {
		return JournalEntry{}, err
	}
	return entry, nil
}

// CreateJournalFile creates or truncates a journal file with a valid header.
func CreateJournalFile(path string, pageSize uint32, entryCount uint32) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if err := WriteJournalHeader(file, JournalHeader{
		Magic:      journalMagic,
		Version:    journalVersion,
		PageSize:   pageSize,
		EntryCount: entryCount,
	}); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

// OpenJournalFile opens an existing rollback journal file for later reads.
func OpenJournalFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR, 0)
}

// WriteRollbackJournal writes original durable page images for pages that are
// about to be overwritten. Commit is not complete until the database is synced
// and this journal is removed.
func WriteRollbackJournal(path string, pageSize uint32, pages []*Page) error {
	file, err := CreateJournalFile(path, pageSize, uint32(len(pages)))
	if err != nil {
		return err
	}
	defer file.Close()

	ordered := make([]*Page, 0, len(pages))
	ordered = append(ordered, pages...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ID() < ordered[j].ID()
	})

	for _, page := range ordered {
		if page == nil || !page.hasOriginal || len(page.originalData) == 0 {
			continue
		}
		if err := WriteJournalEntry(file, uint32(page.ID()), page.originalData); err != nil {
			return err
		}
	}

	if err := file.Sync(); err != nil {
		return err
	}
	return file.Close()
}

// ReadRollbackJournal reads a complete rollback journal or fails cleanly on
// malformed or incomplete data.
func ReadRollbackJournal(path string) (JournalHeader, []JournalEntry, error) {
	file, err := OpenJournalFile(path)
	if err != nil {
		return JournalHeader{}, nil, err
	}
	defer file.Close()

	header, err := ReadJournalHeader(file)
	if err != nil {
		return JournalHeader{}, nil, err
	}

	entries := make([]JournalEntry, 0, header.EntryCount)
	for i := uint32(0); i < header.EntryCount; i++ {
		entry, err := ReadJournalEntry(file, header.PageSize)
		if err != nil {
			return JournalHeader{}, nil, err
		}
		if uint32(len(entry.Data)) != header.PageSize {
			return JournalHeader{}, nil, errInvalidJournal
		}
		entries = append(entries, entry)
	}

	return header, entries, nil
}

// RecoverFromRollbackJournal restores last-committed page images from a
// surviving rollback journal before catalog or row metadata loads. This is
// rollback-journal recovery, not WAL.
func RecoverFromRollbackJournal(dbPath string, pageSize uint32) error {
	journalPath := JournalPath(dbPath)
	if _, err := os.Stat(journalPath); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}

	header, entries, err := ReadRollbackJournal(journalPath)
	if err != nil {
		return err
	}
	if header.PageSize != pageSize {
		return errJournalSizeMismatch
	}

	file, err := os.OpenFile(dbPath, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, entry := range entries {
		if _, err := file.WriteAt(entry.Data, pageOffset(PageID(entry.PageID))); err != nil {
			return err
		}
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := os.Remove(journalPath); err != nil {
		return err
	}
	return nil
}
