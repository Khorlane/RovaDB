package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Khorlane/RovaDB"
)

func main() {
	path := "example.db"
	_ = os.Remove(path)

	db, err := rovadb.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT, active BOOL)"); err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	for _, user := range []struct {
		id     int
		name   string
		active bool
	}{
		{id: 1, name: "Alice", active: true},
		{id: 2, name: "Bob", active: false},
	} {
		if _, err := tx.Exec("INSERT INTO users VALUES (?, ?, ?)", user.id, user.name, user.active); err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
	}

	var activeCount int
	if err := tx.QueryRow("SELECT COUNT(*) FROM users WHERE active = ?", true).Scan(&activeCount); err != nil {
		_ = tx.Rollback()
		log.Fatal(err)
	}
	fmt.Printf("active users in tx: %d\n", activeCount)

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	printActiveUsers(db, "before reopen")

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db, err = rovadb.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	printUserStatusByID(db, 2)
}

func printActiveUsers(db *rovadb.DB, label string) {
	rows, err := db.Query("SELECT id, name FROM users WHERE active = ? ORDER BY id", true)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println(label)
	fmt.Printf("columns: %v\n", rows.Columns())
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d %s\n", id, name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func printUserStatusByID(db *rovadb.DB, id int) {
	row := db.QueryRow("SELECT name, active FROM users WHERE id = ?", id)

	var name string
	var active bool
	if err := row.Scan(&name, &active); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("after reopen: id=%d name=%s active=%v\n", id, name, active)
}
