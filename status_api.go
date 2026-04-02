package rovadb

import (
	"github.com/Khorlane/RovaDB/internal/executor"
	"github.com/Khorlane/RovaDB/internal/storage"
)

// EngineStatus is a cheap diagnostic snapshot of engine control-plane state.
type EngineStatus struct {
	DBFormatVersion         uint32
	WALVersion              uint32
	LastCheckpointLSN       uint64
	LastCheckpointPageCount uint32
	FreeListHead            uint32
	TableCount              int
	IndexCount              int
}

// EngineStatus returns a stable in-memory status snapshot for diagnostics.
func (db *DB) EngineStatus() (EngineStatus, error) {
	if db == nil {
		return EngineStatus{}, ErrInvalidArgument
	}
	if db.closed {
		return EngineStatus{}, ErrClosed
	}
	if err := db.validateTxnState(); err != nil {
		return EngineStatus{}, err
	}

	return EngineStatus{
		DBFormatVersion:         storage.DBFormatVersion(),
		WALVersion:              storage.CurrentWALVersion,
		LastCheckpointLSN:       db.lastCheckpointLSN,
		LastCheckpointPageCount: db.lastCheckpointPageCount,
		FreeListHead:            db.freeListHead,
		TableCount:              statusUserTableCount(db.tables),
		IndexCount:              userIndexCount(db.tables),
	}, nil
}

func statusUserTableCount(tables map[string]*executor.Table) int {
	count := 0
	for _, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		count++
	}
	return count
}

func userIndexCount(tables map[string]*executor.Table) int {
	count := 0
	for _, table := range tables {
		if table == nil || table.IsSystem {
			continue
		}
		count += len(table.IndexDefs)
	}
	return count
}
