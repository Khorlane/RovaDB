package main

import (
	"context"
	"errors"
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

	_, err = db.Exec(context.Background(), "create table users (id integer)")
	if errors.Is(err, rovadb.ErrNotImplemented) {
		fmt.Println("exec is not implemented yet, which is expected at this stage")
		return
	}
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("opened %T\n", db)
}
