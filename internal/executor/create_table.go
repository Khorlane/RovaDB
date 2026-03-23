package executor

import (
	"errors"

	"github.com/Khorlane/RovaDB/internal/parser"
)

var (
	errTableAlreadyExists   = errors.New("executor: table already exists")
	errTableDoesNotExist    = errors.New("executor: table does not exist")
	errWrongValueCount      = errors.New("executor: wrong value count")
	errColumnDoesNotExist   = errors.New("executor: column does not exist")
	errUnsupportedStatement = errors.New("executor: unsupported statement")
)

// Table is the tiny in-memory table catalog entry.
type Table struct {
	Name    string
	Columns []string
	Rows    [][]parser.Value
}

// Execute handles the tiny Stage 1 write statement set.
func Execute(stmt any, tables map[string]*Table) error {
	switch s := stmt.(type) {
	case *parser.CreateTableStmt:
		if _, exists := tables[s.Name]; exists {
			return errTableAlreadyExists
		}
		tables[s.Name] = &Table{
			Name:    s.Name,
			Columns: append([]string(nil), s.Columns...),
		}
		return nil
	case *parser.InsertStmt:
		return executeInsert(s, tables)
	default:
		return errUnsupportedStatement
	}
}
