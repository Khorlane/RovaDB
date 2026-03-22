package main

import (
	"fmt"
	"os"

	"github.com/Khorlane/RovaDB"
)

func main() {
	db, err := rovadb.Open("")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("rovadb CLI stub")
}
