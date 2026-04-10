package main

import (
	"fmt"
	"log"
	"os"

	rovadb "github.com/Khorlane/RovaDB"
)

func main() {
	path := "pkfk_example.db"
	_ = os.Remove(path)

	db, err := rovadb.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE accounts (account_id INT, name TEXT, CONSTRAINT pk_accounts PRIMARY KEY (account_id) USING INDEX idx_accounts_pk)",
		"CREATE TABLE invoices (invoice_id INT, account_id INT, amount REAL, CONSTRAINT pk_invoices PRIMARY KEY (invoice_id) USING INDEX idx_invoices_pk, CONSTRAINT fk_invoices_account FOREIGN KEY (account_id) REFERENCES accounts (account_id) USING INDEX idx_invoices_account ON DELETE CASCADE)",
		"INSERT INTO accounts VALUES (1, 'Acme Co')",
		"INSERT INTO invoices VALUES (1001, 1, 42.50)",
		"INSERT INTO invoices VALUES (1002, 1, 15.00)",
	} {
		if _, err := db.Exec(sql); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("before delete: accounts=%d invoices=%d\n",
		countRows(db, "SELECT COUNT(*) FROM accounts"),
		countRows(db, "SELECT COUNT(*) FROM invoices"),
	)

	if _, err := db.Exec("DELETE FROM accounts WHERE account_id = 1"); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("after cascade delete: accounts=%d invoices=%d\n",
		countRows(db, "SELECT COUNT(*) FROM accounts"),
		countRows(db, "SELECT COUNT(*) FROM invoices"),
	)
}

func countRows(db *rovadb.DB, sql string) int {
	var n int
	if err := db.QueryRow(sql).Scan(&n); err != nil {
		log.Fatal(err)
	}
	return n
}
