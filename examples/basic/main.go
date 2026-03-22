package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Khorlane/RovaDB"
)

func main() {
	db, err := rovadb.Open("example.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query(context.Background(), "SELECT (1 + 2)")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var value int64
		if err := rows.Scan(&value); err != nil {
			log.Fatal(err)
		}
		fmt.Println(value)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
