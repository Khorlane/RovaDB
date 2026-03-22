package main

import (
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

	fmt.Printf("opened %T\n", db)
}
