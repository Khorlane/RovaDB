package rovadb_test

import (
	"testing"
)

func TestAlterTableAddColumnBasic(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"ALTER TABLE users ADD COLUMN age INT",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id, age FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int32
	var age1 any
	if err := rows.Scan(&id1, &age1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || age1 != nil {
		t.Fatalf("first row = (%d, %#v), want (1, nil)", id1, age1)
	}
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int32
	var age2 any
	if err := rows.Scan(&id2, &age2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || age2 != nil {
		t.Fatalf("second row = (%d, %#v), want (2, nil)", id2, age2)
	}
}

func TestAlterTableAddColumnInsertAndUpdate(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"ALTER TABLE users ADD COLUMN age INT",
		"UPDATE users SET age = 30 WHERE id = 1",
		"INSERT INTO users VALUES (2, 'bob', 40)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT id, age FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int32
	var age1 int32
	if err := rows.Scan(&id1, &age1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || age1 != 30 {
		t.Fatalf("first row = (%d, %d), want (1, 30)", id1, age1)
	}
	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int32
	var age2 int32
	if err := rows.Scan(&id2, &age2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || age2 != 40 {
		t.Fatalf("second row = (%d, %d), want (2, 40)", id2, age2)
	}
}

func TestAlterTableAddColumnReopenAndWhere(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"ALTER TABLE users ADD COLUMN age INT",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	rows, err := db.Query("SELECT id FROM users WHERE age = NULL")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("Next() = false, want true")
	}
	var id int32
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}
	if id != 1 {
		t.Fatalf("id = %d, want 1", id)
	}
}

func TestAlterTableUnsupportedForms(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"ALTER TABLE users DROP COLUMN age",
		"ALTER TABLE users ADD age INT",
	} {
		if _, err := db.Exec(sql); err == nil || err.Error() != "parse: unsupported alter table form" {
			t.Fatalf("Exec(%q) error = %v, want %q", sql, err, "parse: unsupported alter table form")
		}
	}
}

func TestAlterTableAddColumnDefaultsAndNotNullPersistAcrossReopen(t *testing.T) {
	path := testDBPath(t)

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for _, sql := range []string{
		"CREATE TABLE users (id INT NOT NULL)",
		"INSERT INTO users VALUES (1)",
		"INSERT INTO users VALUES (2)",
		"ALTER TABLE users ADD COLUMN name TEXT DEFAULT 'ready'",
		"ALTER TABLE users ADD COLUMN active BOOL NOT NULL DEFAULT TRUE",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	db = reopenDB(t, path)
	defer db.Close()

	schema, err := db.GetTableSchema("users")
	if err != nil {
		t.Fatalf("GetTableSchema() error = %v", err)
	}
	if len(schema.Columns) != 3 {
		t.Fatalf("len(GetTableSchema().Columns) = %d, want 3", len(schema.Columns))
	}
	if schema.Columns[1] != (ColumnInfo{Name: "name", Type: "TEXT", HasDefault: true, DefaultValue: "ready"}) {
		t.Fatalf("GetTableSchema().Columns[1] = %#v, want TEXT DEFAULT 'ready'", schema.Columns[1])
	}
	if schema.Columns[2] != (ColumnInfo{Name: "active", Type: "BOOL", NotNull: true, HasDefault: true, DefaultValue: true}) {
		t.Fatalf("GetTableSchema().Columns[2] = %#v, want BOOL NOT NULL DEFAULT TRUE", schema.Columns[2])
	}

	rows, err := db.Query("SELECT id, name, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(existing rows) error = %v", err)
	}

	if !rows.Next() {
		t.Fatal("Next() first = false, want true")
	}
	var id1 int32
	var name1 string
	var active1 bool
	if err := rows.Scan(&id1, &name1, &active1); err != nil {
		t.Fatalf("Scan() first error = %v", err)
	}
	if id1 != 1 || name1 != "ready" || active1 != true {
		t.Fatalf("first row = (%d, %q, %v), want (1, %q, true)", id1, name1, active1, "ready")
	}

	if !rows.Next() {
		t.Fatal("Next() second = false, want true")
	}
	var id2 int32
	var name2 string
	var active2 bool
	if err := rows.Scan(&id2, &name2, &active2); err != nil {
		t.Fatalf("Scan() second error = %v", err)
	}
	if id2 != 2 || name2 != "ready" || active2 != true {
		t.Fatalf("second row = (%d, %q, %v), want (2, %q, true)", id2, name2, active2, "ready")
	}
	if rows.Next() {
		t.Fatal("Next() third = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err() = %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Rows.Close() = %v", err)
	}

	if _, err := db.Exec("INSERT INTO users (id) VALUES (3)"); err != nil {
		t.Fatalf("Exec(insert after reopen) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (4, 'sam', NULL)"); err == nil || err.Error() != "execution: NOT NULL constraint failed: users.active" {
		t.Fatalf("Exec(explicit NULL after reopen) error = %v, want NOT NULL constraint failure", err)
	}

	rows, err = db.Query("SELECT id, name, active FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query(after insert) error = %v", err)
	}
	defer rows.Close()

	want := []struct {
		id     int32
		name   string
		active bool
	}{
		{id: 1, name: "ready", active: true},
		{id: 2, name: "ready", active: true},
		{id: 3, name: "ready", active: true},
	}
	for i, rowWant := range want {
		if !rows.Next() {
			t.Fatalf("Next() row %d = false, want true", i)
		}
		var id int32
		var name string
		var active bool
		if err := rows.Scan(&id, &name, &active); err != nil {
			t.Fatalf("Scan() row %d error = %v", i, err)
		}
		if id != rowWant.id || name != rowWant.name || active != rowWant.active {
			t.Fatalf("row %d = (%d, %q, %v), want (%d, %q, %v)", i, id, name, active, rowWant.id, rowWant.name, rowWant.active)
		}
	}
	if rows.Next() {
		t.Fatal("Next() extra row = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Rows.Err() after insert = %v", err)
	}
}

