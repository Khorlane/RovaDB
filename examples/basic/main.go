package main

import (
	"context"
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
	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INT, name TEXT)"); err != nil {
		log.Fatal(err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(context.Background(), sql); err != nil {
			log.Fatal(err)
		}
	}

	printUsers(db, "before reopen")

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db, err = rovadb.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	printUsers(db, "after reopen")
}

func printUsers(db *rovadb.DB, label string) {
	rows, err := db.Query(context.Background(), "SELECT id, name FROM users")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println(label)
	for rows.Next() {
		var id int64
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
