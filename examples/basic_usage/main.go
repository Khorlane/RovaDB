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
	if _, err := db.Exec("CREATE TABLE users (id INT NOT NULL, name TEXT DEFAULT 'ready', active BOOL NOT NULL DEFAULT TRUE, score REAL DEFAULT 0.0)"); err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	for _, user := range []struct {
		id     int32
		name   string
		active bool
		score  float64
	}{
		{id: 1, name: "Alice", active: false, score: 7.5},
	} {
		if _, err := tx.Exec("INSERT INTO users VALUES (?, ?, ?, ?)", user.id, user.name, user.active, user.score); err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
	}
	if _, err := tx.Exec("INSERT INTO users (id) VALUES (?)", int32(2)); err != nil {
		_ = tx.Rollback()
		log.Fatal(err)
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
	rows, err := db.Query("SELECT id, name, score FROM users WHERE active = ? ORDER BY id", true)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	fmt.Println(label)
	fmt.Printf("columns: %v\n", rows.Columns())
	for rows.Next() {
		var id int
		var name string
		var score float64
		if err := rows.Scan(&id, &name, &score); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d %s score=%0.1f\n", id, name, score)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}

func printUserStatusByID(db *rovadb.DB, id int) {
	row := db.QueryRow("SELECT name, active, score FROM users WHERE id = ?", id)

	var name string
	var active bool
	var score float64
	if err := row.Scan(&name, &active, &score); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("after reopen: id=%d name=%s active=%v score=%0.1f\n", id, name, active, score)
}
