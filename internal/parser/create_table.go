package parser

import (
	"strings"
	"unicode"
)

var errUnsupportedStatement = newParseError("unsupported query form")

const (
	ColumnTypeInt  = "INT"
	ColumnTypeText = "TEXT"
	ColumnTypeBool = "BOOL"
	ColumnTypeReal = "REAL"
)

/*
--- BOOL DESIGN (LOCKED) ---

Schema type:
- Name: BOOL

Runtime value:
- New value kind: Bool
- Go type: bool
- NULL remains separate (existing nil handling unchanged)

Literal forms:
- TRUE, FALSE (case-insensitive if parser already supports it)
- Quoted 'true'/'false' remain TEXT
- No numeric coercion (0/1 are INT, not BOOL)

Type enforcement:
- BOOL columns accept: TRUE, FALSE, NULL
- Reject: INT (0/1), TEXT ('true', etc.)

Storage encoding:
- Introduce a new value kind tag for BOOL
- Encoding:
    TRUE  -> BOOL tag + 1 byte (1)
    FALSE -> BOOL tag + 1 byte (0)
- Must NOT reuse INT or TEXT encoding
- Must remain backward-compatible with existing rows

Comparison semantics:
- TRUE == TRUE only
- FALSE == FALSE only
- No cross-type equality with INT/TEXT
*/

// ColumnDef is the tiny parsed form for a typed column definition.
type ColumnDef struct {
	Name string
	Type string
}

type ForeignKeyDeleteAction string

const (
	ForeignKeyDeleteActionRestrict ForeignKeyDeleteAction = "RESTRICT"
	ForeignKeyDeleteActionCascade  ForeignKeyDeleteAction = "CASCADE"
)

// PrimaryKeyDef is the parsed named PRIMARY KEY table-constraint shape.
type PrimaryKeyDef struct {
	Name      string
	Columns   []string
	IndexName string
}

// ForeignKeyDef is the parsed named FOREIGN KEY table-constraint shape.
type ForeignKeyDef struct {
	Name          string
	Columns       []string
	ParentTable   string
	ParentColumns []string
	IndexName     string
	OnDelete      ForeignKeyDeleteAction
}

// CreateTableStmt is the tiny parsed form for CREATE TABLE.
type CreateTableStmt struct {
	Name        string
	Columns     []ColumnDef
	PrimaryKey  *PrimaryKeyDef
	ForeignKeys []ForeignKeyDef
}

// Parse dispatches the tiny Stage 1 statement shapes.
func Parse(input string) (any, error) {
	trimmed := normalizeSQLInput(input)
	upper := strings.ToUpper(trimmed)

	if strings.HasPrefix(upper, "CREATE TABLE ") {
		return parseCreateTable(trimmed)
	}
	if strings.HasPrefix(upper, "CREATE INDEX ") || strings.HasPrefix(upper, "CREATE UNIQUE INDEX ") {
		return parseCreateIndexTokens(trimmed)
	}
	if strings.HasPrefix(upper, "ALTER TABLE ") {
		return parseAlterTable(trimmed)
	}
	if strings.HasPrefix(upper, "DROP TABLE ") {
		return parseDropTableTokens(trimmed)
	}
	if strings.HasPrefix(upper, "DROP INDEX ") {
		return parseDropIndexTokens(trimmed)
	}
	if strings.HasPrefix(upper, "INSERT INTO ") {
		return parseInsert(trimmed)
	}
	if strings.HasPrefix(upper, "DELETE FROM ") {
		return parseDelete(trimmed)
	}
	if strings.HasPrefix(upper, "UPDATE ") {
		return parseUpdate(trimmed)
	}
	if upper == "COMMIT" {
		return parseCommitTokens(trimmed)
	}
	if upper == "ROLLBACK" {
		return parseRollbackTokens(trimmed)
	}
	if sel, ok := ParseSelectExpr(trimmed); ok {
		return sel, nil
	}

	return nil, errUnsupportedStatement
}

func normalizeSQLInput(input string) string {
	trimmed := strings.TrimSpace(input)
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	return trimmed
}

func parseCreateTable(input string) (*CreateTableStmt, error) {
	return parseCreateTableTokens(input)
}

func isIdentifier(s string) bool {
	if s == "" {
		return false
	}

	for i, r := range s {
		if i == 0 {
			if !unicode.IsLetter(r) && r != '_' {
				return false
			}
			continue
		}
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}

	return true
}
