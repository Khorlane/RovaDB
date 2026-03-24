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
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'Alice', TRUE)",
		"INSERT INTO users VALUES (2, 'Bob', FALSE)",
	} {
		if _, err := db.Exec(sql); err != nil {
			log.Fatal(err)
		}
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
	rows, err := db.Query("SELECT id, name FROM users WHERE active = TRUE ORDER BY id")
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
	row := db.QueryRow(fmt.Sprintf("SELECT name, active FROM users WHERE id = %d", id))

	var name string
	var active bool
	if err := row.Scan(&name, &active); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("after reopen: id=%d name=%s active=%v\n", id, name, active)
}
