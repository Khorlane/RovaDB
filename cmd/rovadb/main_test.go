package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunHelp(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "RovaDB CLI") {
		t.Fatalf("output missing banner:\n%s", output)
	}
	if !strings.Contains(output, "Available commands:") {
		t.Fatalf("output missing help text:\n%s", output)
	}
	if !strings.Contains(output, "help sql") {
		t.Fatalf("output missing help sql command:\n%s", output)
	}
	if strings.Contains(output, "help select|insert|update|delete") {
		t.Fatalf("output should no longer advertise dense SQL topic list:\n%s", output)
	}
	if !strings.Contains(output, "open <path>") {
		t.Fatalf("output missing open command:\n%s", output)
	}
	if !strings.Contains(output, "sample <path>") {
		t.Fatalf("output missing sample command:\n%s", output)
	}
	if !strings.Contains(output, "close") {
		t.Fatalf("output missing close command:\n%s", output)
	}
	if !strings.Contains(output, "db") || !strings.Contains(output, "tables") || !strings.Contains(output, "schema <table>") {
		t.Fatalf("output missing new commands:\n%s", output)
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestBannerShowsRovaDBVersion(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("quit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "RovaDB CLI (v0.14.0)") {
		t.Fatalf("output missing shared version banner:\n%s", out.String())
	}
}

func TestRunHelpInsert(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help insert\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "INSERT example:") {
		t.Fatalf("output missing insert help header:\n%s", output)
	}
	if !strings.Contains(output, "INSERT INTO users VALUES (1, 'alice')") {
		t.Fatalf("output missing simple insert example:\n%s", output)
	}
	if !strings.Contains(output, "INSERT INTO users (id, name) VALUES (1, 'alice')") {
		t.Fatalf("output missing column-list insert example:\n%s", output)
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunHelpUnknownTopicWithoutOpenDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help frobnicate\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, `no help available for "frobnicate"`) {
		t.Fatalf("output missing unknown-topic help message:\n%s", output)
	}
	if strings.Contains(output, "no database is open") {
		t.Fatalf("help topic should not depend on DB state:\n%s", output)
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunHelpUpdate(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help update\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "UPDATE example:") {
		t.Fatalf("output missing update help header:\n%s", output)
	}
	if !strings.Contains(output, "UPDATE users SET name = 'alice' WHERE id = 1") {
		t.Fatalf("output missing simple update example:\n%s", output)
	}
	if !strings.Contains(output, "UPDATE users SET name = 'alice', active = TRUE WHERE id = 1") {
		t.Fatalf("output missing multi-column update example:\n%s", output)
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunHelpDelete(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help delete\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "DELETE example:") {
		t.Fatalf("output missing delete help header:\n%s", output)
	}
	if !strings.Contains(output, "DELETE FROM users WHERE id = 1") {
		t.Fatalf("output missing simple delete example:\n%s", output)
	}
	if !strings.Contains(output, "DELETE FROM users WHERE active = FALSE") {
		t.Fatalf("output missing second delete example:\n%s", output)
	}
	if strings.Contains(output, "DELETE FROM users") && !strings.Contains(output, "WHERE") {
		t.Fatalf("delete help should not encourage full-table wipes:\n%s", output)
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunHelpSelect(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help select\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "SELECT example:") {
		t.Fatalf("output missing select help header:\n%s", output)
	}
	if !strings.Contains(output, "SELECT * FROM users") {
		t.Fatalf("output missing simple select example:\n%s", output)
	}
	if !strings.Contains(output, "SELECT id, name FROM users WHERE active = TRUE ORDER BY id") {
		t.Fatalf("output missing filtered select example:\n%s", output)
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunHelpSQL(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help sql\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	for _, want := range []string{
		"SQL examples:",
		"SELECT * FROM users",
		"INSERT INTO users VALUES (1, 'alice')",
		"UPDATE users SET name = 'alice' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"ALTER TABLE users ADD COLUMN age INT",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunHelpAlter(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help alter\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "ALTER example:") {
		t.Fatalf("output missing alter help header:\n%s", output)
	}
	if !strings.Contains(output, "ALTER TABLE users ADD COLUMN age INT") {
		t.Fatalf("output missing int add-column example:\n%s", output)
	}
	if !strings.Contains(output, "ALTER TABLE users ADD COLUMN active BOOL") {
		t.Fatalf("output missing bool add-column example:\n%s", output)
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunHelpOpen(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help open\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "OPEN example:") || !strings.Contains(out.String(), "open test.db") {
		t.Fatalf("output missing open help:\n%s", out.String())
	}
}

func TestRunHelpSample(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help sample\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "SAMPLE example:") || !strings.Contains(out.String(), "sample test.db") {
		t.Fatalf("output missing sample help:\n%s", out.String())
	}
}

func TestRunHelpClose(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help close\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "CLOSE example:") || !strings.Contains(out.String(), "close") {
		t.Fatalf("output missing close help:\n%s", out.String())
	}
}

func TestRunHelpDB(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help db\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "DB example:") || !strings.Contains(out.String(), "Shows the current open database path") {
		t.Fatalf("output missing db help:\n%s", out.String())
	}
}

func TestRunHelpTables(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help tables\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "TABLES example:") || !strings.Contains(out.String(), "tables") {
		t.Fatalf("output missing tables help:\n%s", out.String())
	}
}

func TestRunHelpSchema(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("help schema\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "SCHEMA example:") || !strings.Contains(out.String(), "schema users") {
		t.Fatalf("output missing schema help:\n%s", out.String())
	}
}

func TestRunExitAliases(t *testing.T) {
	aliases := []string{"quit", "exit", "q", "bye", "\\q"}

	for _, alias := range aliases {
		t.Run(alias, func(t *testing.T) {
			var out bytes.Buffer
			var errOut bytes.Buffer

			code := runWithArgs(strings.NewReader(alias+"\n"), &out, &errOut, nil)
			if code != 0 {
				t.Fatalf("run() code = %d, want 0", code)
			}
			if errOut.Len() != 0 {
				t.Fatalf("errOut = %q, want empty", errOut.String())
			}
		})
	}
}

func TestRunIgnoresBlankLines(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("\n \n\t\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if strings.Count(output, "rovadb> ") != 4 {
		t.Fatalf("prompt count = %d, want 4\noutput:\n%s", strings.Count(output, "rovadb> "), output)
	}
	if strings.Contains(output, "unknown command:") {
		t.Fatalf("blank lines should not produce unknown command output:\n%s", output)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("wat\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "unknown command: wat") {
		t.Fatalf("output missing unknown-command message:\n%s", output)
	}
	if !strings.Contains(output, "type help for commands") {
		t.Fatalf("output missing help hint:\n%s", output)
	}
}

func TestRunTypoedSQLKeywordIsUnknownCommand(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("selet * from users\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "unknown command: selet") {
		t.Fatalf("output missing typoed-sql unknown-command message:\n%s", output)
	}
	if !strings.Contains(output, "type help for commands") {
		t.Fatalf("output missing help hint:\n%s", output)
	}
}

func TestRunDetachedSQLStillShowsOpenGuidance(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("select * from users\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "no database is open") {
		t.Fatalf("output missing no-open message:\n%s", output)
	}
	if !strings.Contains(output, "open an existing database with: open <path>") {
		t.Fatalf("output missing open-shape hint:\n%s", output)
	}
	if !strings.Contains(output, "try: open test.db") {
		t.Fatalf("output missing concrete example hint:\n%s", output)
	}
}

func TestRunEOF(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader(""), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunStartupHint(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("quit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "No database open. Try: open test.db") {
		t.Fatalf("output missing startup hint:\n%s", output)
	}
}

func TestRunInputError(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	input := strings.Repeat("a", bufioMaxScanTokenSize+1)
	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}
	if !strings.Contains(errOut.String(), "input error:") {
		t.Fatalf("errOut missing input error message:\n%s", errOut.String())
	}
}

func TestRunOpenNewDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	code := runWithArgs(strings.NewReader("open "+path+"\ny\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, path+" was not found") {
		t.Fatalf("output missing not-found prompt:\n%s", output)
	}
	if !strings.Contains(output, "create "+path+"? [y/n]") {
		t.Fatalf("output missing create confirmation:\n%s", output)
	}
	if !strings.Contains(output, "opened new "+path) {
		t.Fatalf("output missing new-open message:\n%s", output)
	}
}

func TestRunOpenExistingDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	first := runWithArgs(strings.NewReader("open "+path+"\ny\nquit\n"), &out, &errOut, nil)
	if first != 0 {
		t.Fatalf("first run() code = %d, want 0", first)
	}

	out.Reset()
	errOut.Reset()

	code := runWithArgs(strings.NewReader("open "+path+"\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "opened existing "+path) {
		t.Fatalf("output missing existing-open message:\n%s", output)
	}
}

func TestRunSampleDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "sample.db")
	input := strings.Join([]string{
		"sample " + path,
		"tables",
		"schema customers",
		"SELECT id, name FROM customers ORDER BY id",
		"quit",
		"",
	}, "\n")

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	for _, want := range []string{
		"created sample database " + path,
		"sample tables: customers, orders",
		"customers",
		"orders",
		"table: customers",
		"id INT",
		"name TEXT",
		"city TEXT",
		"id | name          ",
		"---|---------------",
		"1  | Alice Co      ",
		"2  | Bravo Shop    ",
		"3  | Charlie Market",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	if errOut.Len() != 0 {
		t.Fatalf("errOut = %q, want empty", errOut.String())
	}
}

func TestRunSampleRejectsNonEmptyDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "sample.db")
	setup := runWithArgs(strings.NewReader("sample "+path+"\nquit\n"), &out, &errOut, nil)
	if setup != 0 {
		t.Fatalf("setup run() code = %d, want 0", setup)
	}

	out.Reset()
	errOut.Reset()

	code := runWithArgs(strings.NewReader("sample "+path+"\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(errOut.String(), "sample error: database is not empty: "+path) {
		t.Fatalf("errOut missing non-empty sample error:\n%s", errOut.String())
	}
}

func TestRunSampleRejectsWhileDatabaseIsOpen(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	dir := t.TempDir()
	firstPath := filepath.Join(dir, "first.db")
	secondPath := filepath.Join(dir, "sample.db")
	input := "open " + firstPath + "\ny\nsample " + secondPath + "\nquit\n"

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "a database is already open: "+firstPath) {
		t.Fatalf("output missing already-open message:\n%s", output)
	}
	if !strings.Contains(output, "close it before creating a sample database") {
		t.Fatalf("output missing sample guidance message:\n%s", output)
	}
}

func TestRunRejectsOpenWhileDatabaseIsOpen(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	dir := t.TempDir()
	firstPath := filepath.Join(dir, "first.db")
	secondPath := filepath.Join(dir, "second.db")
	input := "open " + firstPath + "\ny\nopen " + secondPath + "\nquit\n"

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "a database is already open: "+firstPath) {
		t.Fatalf("output missing already-open message:\n%s", output)
	}
	if !strings.Contains(output, "close it before opening another database") {
		t.Fatalf("output missing guidance message:\n%s", output)
	}
}

func TestRunOpenMissingDatabaseDeclineCreate(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	code := runWithArgs(strings.NewReader("open "+path+"\nn\ndb\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "did not create "+path) {
		t.Fatalf("output missing decline message:\n%s", output)
	}
	if !strings.Contains(output, "no database is open") {
		t.Fatalf("output missing detached-state confirmation:\n%s", output)
	}
}

func TestRunOpenMissingDatabaseRepromptsOnInvalidConfirmation(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	code := runWithArgs(strings.NewReader("open "+path+"\nmaybe\ny\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "please answer y or n") {
		t.Fatalf("output missing invalid confirmation prompt:\n%s", output)
	}
	if !strings.Contains(output, "opened new "+path) {
		t.Fatalf("output missing eventual open message:\n%s", output)
	}
}

func TestRunCloseWithoutOpenDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("close\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	if !strings.Contains(out.String(), "no database is open") {
		t.Fatalf("output missing no-open close message:\n%s", out.String())
	}
}

func TestRunCloseOpenDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	code := runWithArgs(strings.NewReader("open "+path+"\ny\nclose\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "closed "+path) {
		t.Fatalf("output missing close message:\n%s", output)
	}
}

func TestRunDBWithoutOpenDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader("db\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "no database is open") {
		t.Fatalf("output missing detached db message:\n%s", out.String())
	}
}

func TestRunDBWithOpenDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	code := runWithArgs(strings.NewReader("open "+path+"\ny\ndb\nquit\n"), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "current database: "+path) {
		t.Fatalf("output missing current db path:\n%s", out.String())
	}
}

func TestRunWithStartupPathOpensExistingDatabase(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	setup := runWithArgs(strings.NewReader("open "+path+"\ny\nquit\n"), &out, &errOut, nil)
	if setup != 0 {
		t.Fatalf("setup run() code = %d, want 0", setup)
	}

	out.Reset()
	errOut.Reset()

	code := runWithArgs(strings.NewReader("db\nquit\n"), &out, &errOut, []string{path})
	if code != 0 {
		t.Fatalf("runWithArgs() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "opened existing "+path) {
		t.Fatalf("output missing startup open message:\n%s", output)
	}
	if !strings.Contains(output, "current database: "+path) {
		t.Fatalf("output missing current db message:\n%s", output)
	}
}

func TestRunWithStartupPathMissingRemainsDetached(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "missing.db")
	code := runWithArgs(strings.NewReader("db\nquit\n"), &out, &errOut, []string{path})
	if code != 0 {
		t.Fatalf("runWithArgs() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, path+" was not found") {
		t.Fatalf("output missing startup not-found message:\n%s", output)
	}
	if !strings.Contains(output, "starting with no database open") {
		t.Fatalf("output missing detached startup message:\n%s", output)
	}
	if !strings.Contains(output, "no database is open") {
		t.Fatalf("output missing detached db state:\n%s", output)
	}
}

func TestRunWithUnsupportedStartupFlag(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader(""), &out, &errOut, []string{"--help"})
	if code != 1 {
		t.Fatalf("runWithArgs() code = %d, want 1", code)
	}

	if !strings.Contains(errOut.String(), "unsupported flag: --help") {
		t.Fatalf("errOut missing unsupported-flag message:\n%s", errOut.String())
	}
	if !strings.Contains(errOut.String(), "usage: rovadb [db-path]") {
		t.Fatalf("errOut missing usage message:\n%s", errOut.String())
	}
}

func TestRunWithTooManyStartupArgs(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	code := runWithArgs(strings.NewReader(""), &out, &errOut, []string{"one.db", "two.db"})
	if code != 1 {
		t.Fatalf("runWithArgs() code = %d, want 1", code)
	}

	if !strings.Contains(errOut.String(), "expected at most one database path argument") {
		t.Fatalf("errOut missing extra-args message:\n%s", errOut.String())
	}
	if !strings.Contains(errOut.String(), "usage: rovadb [db-path]") {
		t.Fatalf("errOut missing usage message:\n%s", errOut.String())
	}
}

func TestRunExecPassthrough(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	input := strings.Join([]string{
		"open " + path,
		"y",
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"UPDATE users SET name = 'bob' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"quit",
		"",
	}, "\n")

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if strings.Count(output, "rows affected: 0") < 1 {
		t.Fatalf("output missing rows affected 0 for create:\n%s", output)
	}
	if strings.Count(output, "rows affected: 1") < 3 {
		t.Fatalf("output missing rows affected 1 for write statements:\n%s", output)
	}
}

func TestRunSelectPassthrough(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	input := strings.Join([]string{
		"open " + path,
		"y",
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"SELECT id, name FROM users ORDER BY id",
		"quit",
		"",
	}, "\n")

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "id | name ") {
		t.Fatalf("output missing column header:\n%s", output)
	}
	if !strings.Contains(output, "---|------") {
		t.Fatalf("output missing header separator:\n%s", output)
	}
	if !strings.Contains(output, "1  | alice") || !strings.Contains(output, "2  | bob  ") {
		t.Fatalf("output missing row data:\n%s", output)
	}
}

func TestRunExecPassthroughWithTrailingSemicolon(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	input := strings.Join([]string{
		"open " + path,
		"y",
		"CREATE TABLE users (id INT, name TEXT);",
		"INSERT INTO users VALUES (1, 'alice');",
		"quit",
		"",
	}, "\n")

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if strings.Count(output, "rows affected: 0") < 1 {
		t.Fatalf("output missing semicolon create result:\n%s", output)
	}
	if strings.Count(output, "rows affected: 1") < 1 {
		t.Fatalf("output missing semicolon insert result:\n%s", output)
	}
}

func TestRunSelectPassthroughWithTrailingSemicolon(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	input := strings.Join([]string{
		"open " + path,
		"y",
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"SELECT id, name FROM users ORDER BY id;",
		"quit",
		"",
	}, "\n")

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	if !strings.Contains(output, "id | name ") {
		t.Fatalf("output missing semicolon select header:\n%s", output)
	}
	if !strings.Contains(output, "---|------") {
		t.Fatalf("output missing semicolon select separator:\n%s", output)
	}
	if !strings.Contains(output, "1  | alice") {
		t.Fatalf("output missing semicolon select row:\n%s", output)
	}
}

func TestRunTablesCommand(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	input := strings.Join([]string{
		"open " + path,
		"y",
		"CREATE TABLE users (id INT)",
		"CREATE TABLE teams (id INT)",
		"tables",
		"quit",
		"",
	}, "\n")

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "users") || !strings.Contains(out.String(), "teams") {
		t.Fatalf("output missing table names:\n%s", out.String())
	}
}

func TestRunSchemaCommand(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer

	path := filepath.Join(t.TempDir(), "test.db")
	input := strings.Join([]string{
		"open " + path,
		"y",
		"CREATE TABLE users (id INT, name TEXT, active BOOL, score REAL)",
		"schema users",
		"quit",
		"",
	}, "\n")

	code := runWithArgs(strings.NewReader(input), &out, &errOut, nil)
	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}

	output := out.String()
	for _, want := range []string{"table: users", "id INT", "name TEXT", "active BOOL", "score REAL"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
}

const bufioMaxScanTokenSize = 64 * 1024
