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
	if _, err := db.Exec("CREATE TABLE users (id INT, name TEXT)"); err != nil {
		log.Fatal(err)
	}
	for _, sql := range []string{
		"INSERT INTO users VALUES (1, 'alice')",
		"INSERT INTO users VALUES (2, 'bob')",
	} {
		if _, err := db.Exec(sql); err != nil {
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
	if _, err := db.Query("SELECT id, name FROM users"); err != nil {
		log.Fatal(err)
	}

	fmt.Println(label)
	fmt.Println("SELECT id, name FROM users executed successfully")
}
