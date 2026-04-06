package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	rovadb "github.com/Khorlane/RovaDB"
)

func main() {
	os.Exit(runWithArgs(os.Stdin, os.Stdout, os.Stderr, os.Args[1:]))
}

type cliSession struct {
	db                *rovadb.DB
	path              string
	pendingCreatePath string
}

type commandResult struct {
	handled bool
	exit    bool
}

type helpTopic struct {
	title string
	lines []string
}

var sqlStarterKeywords = map[string]struct{}{
	"select":   {},
	"insert":   {},
	"update":   {},
	"delete":   {},
	"create":   {},
	"alter":    {},
	"drop":     {},
	"commit":   {},
	"rollback": {},
}

var helpTopics = map[string]helpTopic{
	"sql": {
		title: "SQL examples:",
		lines: []string{
			"SELECT * FROM customers",
			"INSERT INTO customers VALUES (4, 'Diana Prince', 'New York')",
			"CREATE INDEX customers_ix2 ON customers (name)",
			"DROP INDEX customers_ix2",
			"UPDATE customers SET city = 'Cambridge' WHERE cust_nbr = 2",
			"DELETE FROM customers WHERE cust_nbr = 1",
			"DROP TABLE customers",
			"ALTER TABLE customers ADD COLUMN status TEXT",
		},
	},
	"select": {
		title: "SELECT example:",
		lines: []string{
			"SELECT * FROM customers",
			"SELECT cust_nbr, name FROM customers WHERE city = 'Boston' ORDER BY cust_nbr",
		},
	},
	"insert": {
		title: "INSERT example:",
		lines: []string{
			"INSERT INTO customers VALUES (4, 'Diana Prince', 'New York')",
			"INSERT INTO customers (cust_nbr, name, city) VALUES (4, 'Diana Prince', 'New York')",
		},
	},
	"update": {
		title: "UPDATE example:",
		lines: []string{
			"UPDATE customers SET name = 'Alice Smith' WHERE cust_nbr = 1",
			"UPDATE customers SET name = 'Brian Carter', city = 'Cambridge' WHERE cust_nbr = 2",
		},
	},
	"delete": {
		title: "DELETE example:",
		lines: []string{
			"DELETE FROM customers WHERE cust_nbr = 1",
			"DELETE FROM customers WHERE city = 'Boston'",
		},
	},
	"alter": {
		title: "ALTER example:",
		lines: []string{
			"ALTER TABLE customers ADD COLUMN status TEXT",
			"ALTER TABLE customers ADD COLUMN credit_limit REAL",
		},
	},
	"open": {
		title: "OPEN example:",
		lines: []string{
			"open test.db",
			"open existing.db",
		},
	},
	"sample": {
		title: "SAMPLE example:",
		lines: []string{
			"sample test.db",
			"Creates sample customers and orders tables",
		},
	},
	"close": {
		title: "CLOSE example:",
		lines: []string{
			"close",
		},
	},
	"db": {
		title: "DB example:",
		lines: []string{
			"db",
			"Shows the current open database path",
		},
	},
	"tables": {
		title: "TABLES example:",
		lines: []string{
			"tables",
		},
	},
	"schema": {
		title: "SCHEMA example:",
		lines: []string{
			"schema customers",
		},
	},
	"version": {
		title: "VERSION example:",
		lines: []string{
			"version",
			"Shows the current RovaDB version",
		},
	},
}

func runWithArgs(in io.Reader, out io.Writer, errOut io.Writer, args []string) int {
	if err := writeBanner(out); err != nil {
		return 1
	}
	if err := writeResponse(out, "Type help for commands."); err != nil {
		return 1
	}
	if err := validateStartupArgs(args); err != nil {
		_ = writeResponse(errOut, "%v", err)
		_ = writeResponse(errOut, "usage: rovadb [db-path]")
		return 1
	}

	session := &cliSession{}
	if len(args) > 0 {
		if err := openStartupPath(out, errOut, session, args[0]); err != nil {
			return 1
		}
	}
	if session.db == nil {
		if err := writeResponse(out, "No database open. Try: open test.db"); err != nil {
			return 1
		}
	}

	scanner := bufio.NewScanner(in)
	for {
		if _, err := fmt.Fprint(out, "rovadb> "); err != nil {
			return 1
		}
		if !scanner.Scan() {
			if _, err := fmt.Fprintln(out); err != nil {
				return 1
			}
			break
		}

		input := strings.TrimSpace(scanner.Text())
		if session.pendingCreatePath != "" {
			if err := handleCreateConfirmation(out, session, input); err != nil {
				if writeErr := writeResponse(errOut, "open error: %v", err); writeErr != nil {
					return 1
				}
			}
			continue
		}

		if input == "" {
			continue
		}

		result, err := handleBuiltInCommand(out, errOut, session, input)
		if err != nil {
			return 1
		}
		if result.exit {
			return 0
		}
		if result.handled {
			continue
		}
		if err := handleSQLOrUnknown(out, errOut, session, input); err != nil {
			return 1
		}
	}

	if err := session.close(); err != nil {
		_ = writeResponse(errOut, "close error: %v", err)
		return 1
	}

	if err := scanner.Err(); err != nil {
		_ = writeResponse(errOut, "input error: %v", err)
		return 1
	}

	return 0
}

func handleBuiltInCommand(out io.Writer, errOut io.Writer, session *cliSession, input string) (commandResult, error) {
	switch {
	case strings.EqualFold(input, "help"):
		return commandResult{handled: true}, printHelp(out)
	case strings.HasPrefix(strings.ToLower(input), "help "):
		topic := strings.TrimSpace(input[len("help "):])
		return commandResult{handled: true}, printHelpTopic(out, topic)
	case strings.EqualFold(input, "db"):
		return commandResult{handled: true}, printCurrentDB(out, session)
	case strings.EqualFold(input, "version"):
		return commandResult{handled: true}, writeResponse(out, "%s", rovadb.Version())
	case strings.EqualFold(input, "close"):
		return commandResult{handled: true}, handleCloseCommand(out, errOut, session)
	case isExitCommand(input):
		if err := session.close(); err != nil {
			_ = writeResponse(errOut, "close error: %v", err)
			return commandResult{}, err
		}
		return commandResult{handled: true, exit: true}, nil
	case strings.EqualFold(input, "open"):
		return commandResult{handled: true}, writeResponse(out, "usage: open <path>")
	case strings.EqualFold(input, "sample"):
		return commandResult{handled: true}, writeResponse(out, "usage: sample <path>")
	case strings.EqualFold(input, "tables"):
		return commandResult{handled: true}, handleTablesCommand(out, errOut, session)
	case strings.EqualFold(input, "schema"):
		return commandResult{handled: true}, writeResponse(out, "usage: schema <table>")
	case strings.HasPrefix(strings.ToLower(input), "schema "):
		return commandResult{handled: true}, handleSchemaCommand(out, errOut, session, input)
	case strings.HasPrefix(strings.ToLower(input), "open "):
		return commandResult{handled: true}, handleOpenCommand(out, errOut, session, input)
	case strings.HasPrefix(strings.ToLower(input), "sample "):
		return commandResult{handled: true}, handleSampleCommand(out, errOut, session, input)
	default:
		return commandResult{}, nil
	}
}

func handleSQLOrUnknown(out io.Writer, errOut io.Writer, session *cliSession, input string) error {
	sqlInput := normalizeCLIQuery(input)
	if !isSQLInput(sqlInput) {
		if err := writeResponse(out, "unknown command: %s", firstToken(input)); err != nil {
			return err
		}
		return writeResponse(out, "type help for commands")
	}
	if session.db == nil {
		return writeDetachedGuidance(out)
	}
	if isSelectQuery(sqlInput) {
		if err := runSelect(out, session.db, sqlInput); err != nil {
			if writeErr := writeResponse(errOut, "query error: %v", err); writeErr != nil {
				return writeErr
			}
		}
		return nil
	}
	if err := runExec(out, session.db, sqlInput); err != nil {
		if writeErr := writeResponse(errOut, "exec error: %v", err); writeErr != nil {
			return writeErr
		}
	}
	return nil
}

func printHelp(out io.Writer) error {
	if err := writeResponse(out, "Available commands:"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "help", "Show this help"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "help sql", "Show example SQL statements"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "open <path>", "Open an existing database"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "sample <path>", "Create and open a sample database"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "close", "Close the current database"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "db", "Show the current database path"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "version", "Show the current RovaDB version"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "tables", "List tables in the open database"); err != nil {
		return err
	}
	if err := writeHelpLine(out, "schema <table>", "Show the schema for one table"); err != nil {
		return err
	}
	return writeHelpLine(out, "quit | exit | q | bye | \\q", "Exit the CLI")
}

func printHelpTopic(out io.Writer, topic string) error {
	name := strings.ToLower(strings.TrimSpace(topic))
	switch name {
	case "":
		return printHelp(out)
	default:
		if topic, ok := helpTopics[name]; ok {
			return printHelpTopicLines(out, topic)
		}
		if err := writeResponse(out, "no help available for %q", topic); err != nil {
			return err
		}
		return writeResponse(out, "type help for the command list")
	}
}

func printHelpTopicLines(out io.Writer, topic helpTopic) error {
	if err := writeResponse(out, topic.title); err != nil {
		return err
	}
	for _, line := range topic.lines {
		if err := writeResponse(out, "  %s", line); err != nil {
			return err
		}
	}
	return nil
}

func handleCloseCommand(out io.Writer, errOut io.Writer, session *cliSession) error {
	if session.path == "" {
		return writeResponse(out, "no database is open")
	}
	path := session.path
	if err := session.close(); err != nil {
		_ = writeResponse(errOut, "close error: %v", err)
		return err
	}
	return writeResponse(out, "closed %s", path)
}

func handleTablesCommand(out io.Writer, errOut io.Writer, session *cliSession) error {
	if session.db == nil {
		return writeResponse(out, "no database is open")
	}
	if err := printTables(out, session.db); err != nil {
		if writeErr := writeResponse(errOut, "tables error: %v", err); writeErr != nil {
			return writeErr
		}
	}
	return nil
}

func handleSchemaCommand(out io.Writer, errOut io.Writer, session *cliSession, input string) error {
	if session.db == nil {
		return writeResponse(out, "no database is open")
	}
	tableName := strings.TrimSpace(input[len("schema "):])
	if tableName == "" {
		return writeResponse(out, "usage: schema <table>")
	}
	if err := printSchema(out, session.db, tableName); err != nil {
		if writeErr := writeResponse(errOut, "schema error: %v", err); writeErr != nil {
			return writeErr
		}
	}
	return nil
}

func handleOpenCommand(out io.Writer, errOut io.Writer, session *cliSession, input string) error {
	path := strings.TrimSpace(input[len("open "):])
	if path == "" {
		return writeResponse(out, "usage: open <path>")
	}
	if session.db != nil {
		if err := writeResponse(out, "a database is already open: %s", session.path); err != nil {
			return err
		}
		return writeResponse(out, "close it before opening another database")
	}
	if !fileExists(path) {
		session.pendingCreatePath = path
		if err := writeResponse(out, "%s was not found", path); err != nil {
			return err
		}
		return writeResponse(out, "create %s? [y/n]", path)
	}
	if err := openExistingPath(session, path); err != nil {
		if writeErr := writeResponse(errOut, "open error: %v", err); writeErr != nil {
			return writeErr
		}
		return nil
	}
	return writeResponse(out, "opened existing %s", path)
}

func handleSampleCommand(out io.Writer, errOut io.Writer, session *cliSession, input string) error {
	path := strings.TrimSpace(input[len("sample "):])
	if path == "" {
		return writeResponse(out, "usage: sample <path>")
	}
	if session.db != nil {
		if err := writeResponse(out, "a database is already open: %s", session.path); err != nil {
			return err
		}
		return writeResponse(out, "close it before creating a sample database")
	}
	if err := createSampleDatabase(session, path); err != nil {
		if writeErr := writeResponse(errOut, "sample error: %v", err); writeErr != nil {
			return writeErr
		}
		return nil
	}
	if err := writeResponse(out, "created sample database %s", path); err != nil {
		return err
	}
	return writeResponse(out, "sample tables: customers, orders")
}

func isExitCommand(input string) bool {
	return strings.EqualFold(input, "quit") ||
		strings.EqualFold(input, "exit") ||
		strings.EqualFold(input, "q") ||
		strings.EqualFold(input, "bye") ||
		input == "\\q"
}

func (s *cliSession) close() error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := s.db.Close(); err != nil {
		return err
	}
	s.db = nil
	s.path = ""
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func validateStartupArgs(args []string) error {
	if len(args) <= 1 {
		if len(args) == 1 && strings.HasPrefix(args[0], "-") {
			return fmt.Errorf("unsupported flag: %s", args[0])
		}
		return nil
	}
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			return fmt.Errorf("unsupported flag: %s", arg)
		}
	}
	return fmt.Errorf("expected at most one database path argument")
}

func isSelectQuery(input string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(input)), "select")
}

func isSQLInput(input string) bool {
	_, ok := sqlStarterKeywords[strings.ToLower(firstToken(input))]
	return ok
}

func normalizeCLIQuery(input string) string {
	trimmed := strings.TrimSpace(input)
	if strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	return trimmed
}

func firstToken(input string) string {
	fields := strings.Fields(strings.TrimSpace(input))
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func printCurrentDB(out io.Writer, session *cliSession) error {
	if session == nil || session.db == nil || session.path == "" {
		return writeResponse(out, "no database is open")
	}
	return writeResponse(out, "current database: %s", session.path)
}

func openStartupPath(out io.Writer, errOut io.Writer, session *cliSession, path string) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if !fileExists(path) {
		if err := writeResponse(out, "%s was not found", path); err != nil {
			return err
		}
		return writeResponse(out, "starting with no database open")
	}
	if err := openExistingPath(session, path); err != nil {
		if writeErr := writeResponse(errOut, "open error: %v", err); writeErr != nil {
			return writeErr
		}
		return writeResponse(out, "starting with no database open")
	}
	return writeResponse(out, "opened existing %s", path)
}

func handleCreateConfirmation(out io.Writer, session *cliSession, input string) error {
	path := session.pendingCreatePath
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "", "n", "no":
		session.pendingCreatePath = ""
		return writeResponse(out, "did not create %s", path)
	case "y", "yes":
		db, err := rovadb.Open(path)
		if err != nil {
			session.pendingCreatePath = ""
			return err
		}
		session.db = db
		session.path = path
		session.pendingCreatePath = ""
		return writeResponse(out, "opened new %s", path)
	default:
		return writeResponse(out, "please answer y or n")
	}
}

func openExistingPath(session *cliSession, path string) error {
	db, err := rovadb.Open(path)
	if err != nil {
		return err
	}
	session.db = db
	session.path = path
	return nil
}

func createSampleDatabase(session *cliSession, path string) error {
	db, err := rovadb.Open(path)
	if err != nil {
		return err
	}

	tables, err := db.ListTables()
	if err != nil {
		_ = db.Close()
		return err
	}
	if len(tables) != 0 {
		_ = db.Close()
		return fmt.Errorf("database is not empty: %s", path)
	}

	for _, sql := range sampleDatabaseSQL() {
		if _, err := db.Exec(sql); err != nil {
			_ = db.Close()
			return err
		}
	}

	session.db = db
	session.path = path
	return nil
}

func sampleDatabaseSQL() []string {
	return []string{
		"CREATE TABLE customers (cust_nbr INT, name TEXT, city TEXT)",
		"CREATE UNIQUE INDEX customers_ix1 ON customers (cust_nbr)",
		"CREATE TABLE orders (cust_nbr INT, order_nbr INT, item TEXT, total_amt REAL)",
		"CREATE UNIQUE INDEX orders_ix1 ON orders (cust_nbr, order_nbr)",
		"INSERT INTO customers VALUES (1, 'Alice Carter', 'Boston')",
		"INSERT INTO customers VALUES (2, 'Brian Lewis', 'Chicago')",
		"INSERT INTO customers VALUES (3, 'Carla Gomez', 'Denver')",
		"INSERT INTO orders VALUES (1, 1, 'Paper', 12.50)",
		"INSERT INTO orders VALUES (1, 2, 'Pens', 8.25)",
		"INSERT INTO orders VALUES (2, 3, 'Stapler', 15.00)",
		"INSERT INTO orders VALUES (3, 4, 'Notebook', 6.75)",
	}
}

func runExec(out io.Writer, db *rovadb.DB, query string) error {
	result, err := db.Exec(query)
	if err != nil {
		return err
	}
	return writeResponse(out, "rows affected: %d", result.RowsAffected())
}

func runSelect(out io.Writer, db *rovadb.DB, query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()

	columns := rows.Columns()
	renderedRows := make([][]string, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		renderedRows = append(renderedRows, formatRowValues(values))
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(columns) == 0 {
		return writeResponse(out, "(no columns)")
	}

	widths := columnWidths(columns, renderedRows)
	if err := writeFormattedHeader(out, columns, widths); err != nil {
		return err
	}
	if len(renderedRows) == 0 {
		return writeResponse(out, "(no rows)")
	}
	for _, row := range renderedRows {
		if err := writeResponse(out, formatTableLine(row, widths, " ")); err != nil {
			return err
		}
	}
	return nil
}

func writeFormattedHeader(out io.Writer, columns []string, widths []int) error {
	if err := writeResponse(out, formatTableLine(columns, widths, " ")); err != nil {
		return err
	}
	return writeResponse(out, formatTableLine(dashedCells(widths), widths, "-"))
}

func formatRowValues(values []any) []string {
	cells := make([]string, len(values))
	for i, value := range values {
		cells[i] = formatValue(value)
	}
	return cells
}

func columnWidths(columns []string, renderedRows [][]string) []int {
	widths := make([]int, len(columns))
	for i, column := range columns {
		widths[i] = len(column)
	}
	for _, row := range renderedRows {
		for i := range columns {
			if i >= len(row) {
				continue
			}
			if len(row[i]) > widths[i] {
				widths[i] = len(row[i])
			}
		}
	}
	return widths
}

func dashedCells(widths []int) []string {
	cells := make([]string, len(widths))
	for i, width := range widths {
		cells[i] = strings.Repeat("-", width)
	}
	return cells
}

func formatTableLine(cells []string, widths []int, pad string) string {
	parts := make([]string, len(cells))
	for i := range cells {
		cell := cells[i]
		width := widths[i]
		if pad == "-" {
			parts[i] = strings.Repeat("-", width)
			continue
		}
		if len(cell) < width {
			cell += strings.Repeat(pad, width-len(cell))
		}
		parts[i] = cell
	}
	separator := " | "
	if pad == "-" {
		separator = "-|-"
	}
	return strings.Join(parts, separator)
}

func formatValue(value any) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", value)
}

func writeResponse(w io.Writer, format string, args ...any) error {
	_, err := fmt.Fprintf(w, "  "+format+"\n", args...)
	return err
}

func writeHelpLine(w io.Writer, command string, description string) error {
	return writeResponse(w, "%-32s %s", command, description)
}

func writeDetachedGuidance(w io.Writer) error {
	if err := writeResponse(w, "no database is open"); err != nil {
		return err
	}
	if err := writeResponse(w, "open an existing database with: open <path>"); err != nil {
		return err
	}
	return writeResponse(w, "try: open test.db")
}

func writeBanner(w io.Writer) error {
	if _, err := fmt.Fprintln(w, "+---------------------+"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "| RovaDB CLI (%s) |\n", rovadb.Version()); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w, "+---------------------+")
	return err
}
