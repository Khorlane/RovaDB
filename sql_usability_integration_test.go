package rovadb

import (
	"strings"
	"testing"
)

func TestSQLUsabilityMilestoneJoinAndAliasSurface(t *testing.T) {
	db, err := Open(testDBPath(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE customers (cust_nbr INT, name TEXT, city TEXT)",
		"CREATE UNIQUE INDEX customers_ix1 ON customers (cust_nbr)",
		"CREATE TABLE orders (cust_nbr INT, order_nbr INT, total_amt INT)",
		"CREATE UNIQUE INDEX orders_ix1 ON orders (cust_nbr, order_nbr)",
		"INSERT INTO customers VALUES (1, 'Alice Carter', 'Boston')",
		"INSERT INTO customers VALUES (2, 'Brian Lewis', 'Chicago')",
		"INSERT INTO customers VALUES (3, 'Carla Gomez', 'Denver')",
		"INSERT INTO orders VALUES (1, 101, 75)",
		"INSERT INTO orders VALUES (1, 102, 25)",
		"INSERT INTO orders VALUES (2, 103, 60)",
		"INSERT INTO orders VALUES (3, 104, 10)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	explicitRows, err := db.Query("SELECT a.cust_nbr AS customer_number, a.name, b.order_nbr, b.total_amt FROM customers a JOIN orders b ON a.cust_nbr = b.cust_nbr WHERE b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query(explicit join) error = %v", err)
	}
	defer explicitRows.Close()
	if got := explicitRows.columns; len(got) != 4 || got[0] != "a.cust_nbr AS customer_number" || got[1] != "a.name" || got[2] != "b.order_nbr" || got[3] != "b.total_amt" {
		t.Fatalf("explicitRows.columns = %#v, want [a.cust_nbr AS customer_number a.name b.order_nbr b.total_amt]", got)
	}
	wantExplicit := [][]any{
		{1, "Alice Carter", 101, 75},
		{2, "Brian Lewis", 103, 60},
	}
	assertMaterializedRowsEqual(t, explicitRows.data, wantExplicit)

	commaRows, err := db.Query("SELECT a.cust_nbr AS customer_number, a.name, b.order_nbr, b.total_amt FROM customers a, orders b WHERE a.cust_nbr = b.cust_nbr AND b.total_amt > 50 ORDER BY a.name")
	if err != nil {
		t.Fatalf("Query(comma join) error = %v", err)
	}
	defer commaRows.Close()
	if got := commaRows.columns; len(got) != len(explicitRows.columns) {
		t.Fatalf("commaRows.columns = %#v, want %#v", got, explicitRows.columns)
	}
	for i := range explicitRows.columns {
		if commaRows.columns[i] != explicitRows.columns[i] {
			t.Fatalf("commaRows.columns[%d] = %q, want %q", i, commaRows.columns[i], explicitRows.columns[i])
		}
	}
	assertMaterializedRowsEqual(t, commaRows.data, explicitRows.data)

	aliasRows, err := db.Query("SELECT cust_nbr AS customer_number, name FROM customers ORDER BY customer_number")
	if err != nil {
		t.Fatalf("Query(alias order by) error = %v", err)
	}
	defer aliasRows.Close()
	if got := aliasRows.columns; len(got) != 2 || got[0] != "customer_number" || got[1] != "name" {
		t.Fatalf("aliasRows.columns = %#v, want [customer_number name]", got)
	}
	wantAlias := [][]any{
		{1, "Alice Carter"},
		{2, "Brian Lewis"},
		{3, "Carla Gomez"},
	}
	assertMaterializedRowsEqual(t, aliasRows.data, wantAlias)
}

func TestSQLUsabilityMilestoneCatalogAndErrorSurface(t *testing.T) {
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
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("Exec(%q) error = %v", sql, err)
		}
	}

	sysRows, err := db.Query("SELECT table_name FROM sys_tables ORDER BY table_name")
	if err != nil {
		t.Fatalf("Query(sys_tables) error = %v", err)
	}
	defer sysRows.Close()
	if got := sysRows.columns; len(got) != 1 || got[0] != "table_name" {
		t.Fatalf("sysRows.columns = %#v, want [table_name]", got)
	}
	wantSys := [][]any{
		{"customers"},
		{"orders"},
	}
	assertMaterializedRowsEqual(t, sysRows.data, wantSys)

	oldRows, err := db.Query("SELECT table_name FROM __sys_tables")
	if err != nil {
		t.Fatalf("Query(__sys_tables) error = %v, want deferred error rowset", err)
	}
	defer oldRows.Close()
	if oldRows.Next() {
		t.Fatalf("oldRows.Next() = true, want false")
	}
	if oldRows.Err() == nil || oldRows.Err().Error() != "execution: table not found: __sys_tables" {
		t.Fatalf("oldRows.Err() = %v, want %q", oldRows.Err(), "execution: table not found: __sys_tables")
	}

	missingRows, err := db.Query("SELECT missing_col FROM customers")
	if err != nil {
		t.Fatalf("Query(missing column) error = %v, want deferred error rowset", err)
	}
	defer missingRows.Close()
	if missingRows.Next() {
		t.Fatalf("missingRows.Next() = true, want false")
	}
	if missingRows.Err() == nil || !strings.Contains(missingRows.Err().Error(), "missing_col") {
		t.Fatalf("missingRows.Err() = %v, want error containing %q", missingRows.Err(), "missing_col")
	}
}

func assertMaterializedRowsEqual(t *testing.T, got [][]any, want [][]any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d (%#v)", len(got), len(want), got)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("row %d width = %d, want %d (%#v)", i, len(got[i]), len(want[i]), got[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("row %d col %d = %#v, want %#v", i, j, got[i][j], want[i][j])
			}
		}
	}
}
