package rovadb

import (
	"errors"
	"testing"
)

func TestQueryAPILiteralSelectReturnsRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if rows.idx != -1 {
		t.Fatalf("rows.idx = %d, want -1", rows.idx)
	}
	if len(rows.columns) != 0 {
		t.Fatalf("rows.columns = %#v, want nil/empty", rows.columns)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 1 {
		t.Fatalf("rows.data = %#v, want [[1]]", rows.data)
	}
}

func TestQueryAPINoArgsStillWorksWithVariadicSignature(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 1 {
		t.Fatalf("rows.data = %#v, want [[1]]", rows.data)
	}
}

func TestQueryAPISelectFromReturnsMaterializedRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if len(rows.columns) != 2 || rows.columns[0] != "id" || rows.columns[1] != "name" {
		t.Fatalf("rows.columns = %#v, want [id name]", rows.columns)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 2 || rows.data[0][0] != 1 || rows.data[0][1] != "alice" {
		t.Fatalf("rows.data = %#v, want [[1 \"alice\"]]", rows.data)
	}
}

func TestQueryAPITextComparisonsAreCaseInsensitiveAcrossWhereAndOrderBy(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'Alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'Charles')",
		"INSERT INTO users VALUES (4, 'BOB')",
		"INSERT INTO users VALUES (5, 'Bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	ordered, err := db.Query("SELECT name FROM users ORDER BY name")
	if err != nil {
		t.Fatalf("Query(order) error = %v", err)
	}
	if ordered == nil || len(ordered.data) < 3 {
		t.Fatalf("ordered rows = %#v, want materialized ordered rows", ordered)
	}
	if ordered.data[0][0] != "Alice" || ordered.data[1][0] != "bob" || ordered.data[2][0] != "BOB" {
		t.Fatalf("ordered rows = %#v, want Alice then bob-group before Charles", ordered.data)
	}

	filtered, err := db.Query("SELECT name FROM users WHERE name = 'bob' ORDER BY id")
	if err != nil {
		t.Fatalf("Query(where) error = %v", err)
	}
	if filtered == nil || len(filtered.data) != 3 {
		t.Fatalf("filtered rows = %#v, want 3 bob matches", filtered)
	}
	if filtered.data[0][0] != "bob" || filtered.data[1][0] != "BOB" || filtered.data[2][0] != "Bob" {
		t.Fatalf("filtered rows = %#v, want bob/BOB/Bob", filtered.data)
	}
}

func TestQueryAPICountStarStillReturnsRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil {
		t.Fatal("Query() rows = nil, want value")
	}
	if len(rows.columns) != 1 || rows.columns[0] != "count" {
		t.Fatalf("rows.columns = %#v, want [count]", rows.columns)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 1 {
		t.Fatalf("rows.data = %#v, want [[1]]", rows.data)
	}
}

func TestQueryAPIEligibleCountStarUsesIndexOnlyWithoutBaseRowFetch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful count rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 3 {
		t.Fatalf("rows.data = %#v, want [[3]]", rows.data)
	}
}

func TestQueryAPIEligibleCountStarOnEmptyIndexedTableReturnsZero(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful count rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 0 {
		t.Fatalf("rows.data = %#v, want [[0]]", rows.data)
	}
}

func TestQueryAPIEligibleCountStarTracksInsertAndDeleteChanges(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
		"DELETE FROM users WHERE id = 2",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful count rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 2 {
		t.Fatalf("rows.data = %#v, want [[2]]", rows.data)
	}
}

func TestQueryAPIEligibleIndexedProjectionUsesIndexOnlyWithoutBaseRowFetch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() = %#v, want [id]", got)
	}
	if len(rows.data) != 3 || rows.data[0][0] != 1 || rows.data[1][0] != 2 || rows.data[2][0] != 3 {
		t.Fatalf("rows.data = %#v, want [[1] [2] [3]]", rows.data)
	}
}

func TestQueryAPIEligibleIndexedProjectionEmptyTableReturnsNoRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "id" {
		t.Fatalf("Columns() = %#v, want [id]", got)
	}
	if len(rows.data) != 0 {
		t.Fatalf("rows.data = %#v, want empty rowset", rows.data)
	}
}

func TestQueryAPIEligibleQualifiedIndexedProjectionWorks(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT users.id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if got := rows.Columns(); len(got) != 1 || got[0] != "users.id" {
		t.Fatalf("Columns() = %#v, want [users.id]", got)
	}
	if len(rows.data) != 2 || rows.data[0][0] != 1 || rows.data[1][0] != 2 {
		t.Fatalf("rows.data = %#v, want [[1] [2]]", rows.data)
	}
}

func TestQueryAPINonIndexedProjectionStillUsesExistingPath(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	table := db.tables["users"]
	if table == nil {
		t.Fatal("db.tables[users] = nil, want value")
	}
	table.SetStorageMeta(0, table.PersistedRowCount())

	rows, err := db.Query("SELECT name FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		t.Fatal("Next() = true, want false")
	}
	if rows.Err() == nil {
		t.Fatal("Err() = nil, want base-row path failure after table root removal")
	}
}

func TestQueryAPIEligibleIndexedProjectionTracksInsertAndDeleteChanges(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("CREATE INDEX users_ix1 ON users (id)"); err != nil {
		t.Fatalf("Exec(create index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'cara')",
		"DELETE FROM users WHERE id = 2",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer rows.Close()

	if len(rows.data) != 2 || rows.data[0][0] != 1 || rows.data[1][0] != 3 {
		t.Fatalf("rows.data = %#v, want [[1] [3]]", rows.data)
	}
}

func TestQueryAPINonSelectRejected(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	tests := []string{
		"CREATE TABLE users (id INT, name TEXT)",
		"INSERT INTO users VALUES (1, 'alice')",
		"UPDATE users SET name = 'bob'",
		"DELETE FROM users",
		"ALTER TABLE users ADD COLUMN age INT",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			rows, err := db.Query(sql)
			if !errors.Is(err, ErrQueryRequiresSelect) {
				t.Fatalf("Query(%q) error = %v, want ErrQueryRequiresSelect", sql, err)
			}
			if rows != nil {
				t.Fatalf("Query(%q) rows = %v, want nil", sql, rows)
			}
		})
	}
}

func TestQueryAPICommaJoinReturnsRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE customers (cust_nbr INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create customers) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX customers_ix1 ON customers (cust_nbr)"); err != nil {
		t.Fatalf("Exec(create customers index) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE orders (cust_nbr INT, order_nbr INT, total_amt INT)"); err != nil {
		t.Fatalf("Exec(create orders) error = %v", err)
	}
	if _, err := db.Exec("CREATE UNIQUE INDEX orders_ix1 ON orders (cust_nbr, order_nbr)"); err != nil {
		t.Fatalf("Exec(create orders index) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO customers VALUES (1, 'alice')",
		"INSERT INTO customers VALUES (2, 'bob')",
		"INSERT INTO orders VALUES (1, 101, 75)",
		"INSERT INTO orders VALUES (1, 102, 25)",
		"INSERT INTO orders VALUES (2, 103, 60)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt FROM customers a, orders b WHERE a.cust_nbr = b.cust_nbr AND b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful joined rowset", rows)
	}
	if len(rows.columns) != 4 || rows.columns[0] != "a.cust_nbr" || rows.columns[1] != "a.name" || rows.columns[2] != "b.order_nbr" || rows.columns[3] != "b.total_amt" {
		t.Fatalf("rows.columns = %#v, want qualified projected columns", rows.columns)
	}
	if len(rows.data) != 2 {
		t.Fatalf("rows.data = %#v, want two joined rows", rows.data)
	}
	if rows.data[0][0] != 1 || rows.data[0][1] != "alice" || rows.data[0][2] != 101 || rows.data[0][3] != 75 {
		t.Fatalf("rows.data[0] = %#v, want [1 alice 101 75]", rows.data[0])
	}
	if rows.data[1][0] != 2 || rows.data[1][1] != "bob" || rows.data[1][2] != 103 || rows.data[1][3] != 60 {
		t.Fatalf("rows.data[1] = %#v, want [2 bob 103 60]", rows.data[1])
	}
}

func TestQueryAPIExplicitJoinReturnsRows(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, dept_id INT)"); err != nil {
		t.Fatalf("Exec(create users) error = %v", err)
	}
	if _, err := db.Exec("CREATE TABLE accounts (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create accounts) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice', 10)",
		"INSERT INTO users VALUES (2, 'bob', 20)",
		"INSERT INTO accounts VALUES (10, 'eng')",
		"INSERT INTO accounts VALUES (20, 'ops')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT u.name, a.name FROM users u JOIN accounts a ON u.dept_id = a.id ORDER BY u.id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful joined rowset", rows)
	}
	if len(rows.data) != 2 || len(rows.data[0]) != 2 {
		t.Fatalf("rows.data = %#v, want two joined rows", rows.data)
	}
	if rows.data[0][0] != "alice" || rows.data[0][1] != "eng" || rows.data[1][0] != "bob" || rows.data[1][1] != "ops" {
		t.Fatalf("rows.data = %#v, want [[alice eng] [bob ops]]", rows.data)
	}
}

func TestQueryAPICommaJoinAndExplicitJoinMatch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE customers (cust_nbr INT, name TEXT)",
		"CREATE UNIQUE INDEX customers_ix1 ON customers (cust_nbr)",
		"CREATE TABLE orders (cust_nbr INT, order_nbr INT, total_amt INT)",
		"CREATE UNIQUE INDEX orders_ix1 ON orders (cust_nbr, order_nbr)",
		"INSERT INTO customers VALUES (1, 'alice')",
		"INSERT INTO customers VALUES (2, 'bob')",
		"INSERT INTO orders VALUES (1, 101, 75)",
		"INSERT INTO orders VALUES (1, 102, 25)",
		"INSERT INTO orders VALUES (2, 103, 60)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	commaRows, err := db.Query("SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt FROM customers a, orders b WHERE a.cust_nbr = b.cust_nbr AND b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query(comma join) error = %v", err)
	}
	explicitRows, err := db.Query("SELECT a.cust_nbr, a.name, b.order_nbr, b.total_amt FROM customers a JOIN orders b ON a.cust_nbr = b.cust_nbr WHERE b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query(explicit join) error = %v", err)
	}
	if commaRows == nil || explicitRows == nil {
		t.Fatalf("rows = (%#v, %#v), want non-nil rowsets", commaRows, explicitRows)
	}
	if len(commaRows.columns) != len(explicitRows.columns) {
		t.Fatalf("column lengths = (%d, %d), want match", len(commaRows.columns), len(explicitRows.columns))
	}
	for i := range commaRows.columns {
		if commaRows.columns[i] != explicitRows.columns[i] {
			t.Fatalf("columns differ at %d: %q vs %q", i, commaRows.columns[i], explicitRows.columns[i])
		}
	}
	if len(commaRows.data) != len(explicitRows.data) {
		t.Fatalf("row counts = (%d, %d), want match", len(commaRows.data), len(explicitRows.data))
	}
	for i := range commaRows.data {
		if len(commaRows.data[i]) != len(explicitRows.data[i]) {
			t.Fatalf("row %d widths = (%d, %d), want match", i, len(commaRows.data[i]), len(explicitRows.data[i]))
		}
		for j := range commaRows.data[i] {
			if commaRows.data[i][j] != explicitRows.data[i][j] {
				t.Fatalf("row %d col %d differ: %v vs %v", i, j, commaRows.data[i][j], explicitRows.data[i][j])
			}
		}
	}
}

func TestQueryAPIPlaceholderArgsWhereClause(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 {
		t.Fatalf("rows = %#v, want one row with one column", rows)
	}
	if rows.data[0][0] != "alice" {
		t.Fatalf("rows.data = %#v, want [[\"alice\"]]", rows.data)
	}
}

func TestQueryAPIPlaceholderArgsBoolWhereClause(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, active BOOL, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, TRUE, 'alice')"); err != nil {
		t.Fatalf("Exec(insert 1) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (2, FALSE, 'bob')"); err != nil {
		t.Fatalf("Exec(insert 2) error = %v", err)
	}

	rows, err := db.Query("SELECT name FROM users WHERE active = ?", true)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 {
		t.Fatalf("rows = %#v, want one row with one column", rows)
	}
	if rows.data[0][0] != "alice" {
		t.Fatalf("rows.data = %#v, want [[\"alice\"]]", rows.data)
	}
}

func TestQueryAPILiteralAndBoundQueriesMatch(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	if _, err := db.Exec("INSERT INTO users VALUES (1, 'alice')"); err != nil {
		t.Fatalf("Exec(insert) error = %v", err)
	}

	literalRows, err := db.Query("SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Query(literal) error = %v", err)
	}
	boundRows, err := db.Query("SELECT name FROM users WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("Query(bound) error = %v", err)
	}

	if literalRows == nil || boundRows == nil {
		t.Fatalf("literalRows = %#v, boundRows = %#v, want values", literalRows, boundRows)
	}
	if len(literalRows.data) != len(boundRows.data) {
		t.Fatalf("len(literalRows.data) = %d, len(boundRows.data) = %d, want equal", len(literalRows.data), len(boundRows.data))
	}
	if len(literalRows.data) != 1 || len(literalRows.data[0]) != 1 || len(boundRows.data[0]) != 1 {
		t.Fatalf("literalRows.data = %#v, boundRows.data = %#v, want one matching row", literalRows.data, boundRows.data)
	}
	if literalRows.data[0][0] != boundRows.data[0][0] {
		t.Fatalf("literalRows.data = %#v, boundRows.data = %#v, want equal", literalRows.data, boundRows.data)
	}
}

func TestQueryAPIPlaceholderArgsRespectBooleanPrecedence(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
		"INSERT INTO users VALUES (3, 'alice')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users WHERE id = ? OR id = ? AND name = ? ORDER BY id", 1, 2, "bob")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 2 {
		t.Fatalf("rows.data = %#v, want two rows", rows.data)
	}
	if rows.data[0][0] != 1 || rows.data[1][0] != 2 {
		t.Fatalf("rows.data = %#v, want [[1] [2]]", rows.data)
	}
}

func TestQueryAPIPlaceholderArgsWithinFunctionOperand(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'ALICE')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id FROM users WHERE LOWER(name) = LOWER(?)", "BOB")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || rows.data[0][0] != 2 {
		t.Fatalf("rows.data = %#v, want [[2]]", rows.data)
	}
}

func TestQueryAPIPlaceholderArgsCountMismatchTooFew(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT 1 WHERE 1 = ?")
	if err != nil {
		t.Fatalf("Query() error = %v, want nil top-level error", err)
	}
	if rows == nil || rows.err == nil {
		t.Fatalf("rows = %#v, want deferred bind error", rows)
	}
}

func TestQueryAPIPlaceholderArgsCountMismatchTooMany(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT name FROM users", 1)
	if err != nil {
		t.Fatalf("Query() error = %v, want nil top-level error", err)
	}
	if rows == nil || rows.err == nil {
		t.Fatalf("rows = %#v, want deferred bind error", rows)
	}
}

func TestQueryAPICountStarWithPlaceholderWhereClause(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, active BOOL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, TRUE)",
		"INSERT INTO users VALUES (2, FALSE)",
		"INSERT INTO users VALUES (3, TRUE)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(*) FROM users WHERE active = ?", true)
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 2 {
		t.Fatalf("rows.data = %#v, want [[2]]", rows.data)
	}
}

func TestQueryAPIAggregateFunctionsReturnSingleRow(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE metrics (id INT, name TEXT, score REAL)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO metrics VALUES (1, 'beta', 1.5)",
		"INSERT INTO metrics VALUES (2, 'alpha', 2.5)",
		"INSERT INTO metrics VALUES (3, 'gamma', 3.0)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT COUNT(name), AVG(score), SUM(score), MIN(name), MAX(score) FROM metrics")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want aggregate rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 5 {
		t.Fatalf("rows.data = %#v, want one aggregate row", rows.data)
	}
	if rows.data[0][0] != 3 || rows.data[0][1] != (1.5+2.5+3.0)/3.0 || rows.data[0][2] != 7.0 || rows.data[0][3] != "alpha" || rows.data[0][4] != 3.0 {
		t.Fatalf("rows.data = %#v, want [[3 2.333... 7 alpha 3.0]]", rows.data)
	}
}

func TestQueryAPIArithmeticProjectionAndPredicate(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id + 1 FROM users WHERE id + 1 = 3")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || len(rows.data) != 1 || len(rows.data[0]) != 1 || rows.data[0][0] != 3 {
		t.Fatalf("rows.data = %#v, want [[3]]", rows.data)
	}
}

func TestQueryAPIAlternateNotEqualsWhereClause(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		t.Fatalf("Exec(create) error = %v", err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT id, name FROM users WHERE id <> 1 ORDER BY id")
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if rows == nil || rows.err != nil {
		t.Fatalf("rows = %#v, want successful rowset", rows)
	}
	if len(rows.data) != 1 || len(rows.data[0]) != 2 {
		t.Fatalf("rows.data = %#v, want one row with two columns", rows.data)
	}
	if rows.data[0][0] != 2 || rows.data[0][1] != "bob" {
		t.Fatalf("rows.data = %#v, want [[2 \"bob\"]]", rows.data)
	}
}

func TestQueryAPIRejectsPlaceholderOutsideValuePositionThroughPublicPath(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (? INT)"); err == nil {
		t.Fatal("Exec(CREATE TABLE t (? INT)) error = nil, want parse error")
	}
}
