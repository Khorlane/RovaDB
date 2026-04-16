package rovadb_test

import rovadb "github.com/Khorlane/RovaDB"

type DB = rovadb.DB
type Tx = rovadb.Tx
type Rows = rovadb.Rows
type Row = rovadb.Row
type Result = rovadb.Result
type TableInfo = rovadb.TableInfo
type ColumnInfo = rovadb.ColumnInfo
type QueryExecutionTrace = rovadb.QueryExecutionTrace
type EngineStatus = rovadb.EngineStatus
type EngineSnapshot = rovadb.EngineSnapshot
type EngineCheckResult = rovadb.EngineCheckResult
type EngineTableInfo = rovadb.EngineTableInfo
type EngineIndexInfo = rovadb.EngineIndexInfo
type EngineSchemaInventory = rovadb.EngineSchemaInventory
type EnginePageUsage = rovadb.EnginePageUsage
type ErrorKind = rovadb.ErrorKind
type OpenOptions = rovadb.OpenOptions

const (
	ErrParse   = rovadb.ErrParse
	ErrPlan    = rovadb.ErrPlan
	ErrExec    = rovadb.ErrExec
	ErrStorage = rovadb.ErrStorage
)

var (
	ErrNotImplemented      = rovadb.ErrNotImplemented
	ErrTxnAlreadyActive    = rovadb.ErrTxnAlreadyActive
	ErrQueryRequiresSelect = rovadb.ErrQueryRequiresSelect
	ErrExecDisallowsSelect = rovadb.ErrExecDisallowsSelect
	ErrNoRows              = rovadb.ErrNoRows
	ErrMultipleRows        = rovadb.ErrMultipleRows
)

func Open(path string) (*DB, error) {
	return rovadb.Open(path)
}

func Create(path string) (*DB, error) {
	return rovadb.Create(path)
}

func OpenWithOptions(path string, opts OpenOptions) (*DB, error) {
	return rovadb.OpenWithOptions(path, opts)
}

func CreateWithOptions(path string, opts OpenOptions) (*DB, error) {
	return rovadb.CreateWithOptions(path, opts)
}

func Version() string {
	return rovadb.Version()
}

