package main

import (
	"fmt"

	"treds/store"
)

func main() {
	store := store.NewTredsStore()
	err := store.DCreateCollection([]string{"users", "{\"name\": {\"type\": \"string\"}, \"age\": {\"type\": \"float\", \"min\": 18}}", "[{\"fields\": [\"name\", \"age\"]}]"})
	if err != nil {
		fmt.Println(err)
	}
	store.DInsert([]string{"users", "{\"name\": \"John Doe\", \"age\": 30}"})
	store.DInsert([]string{"users", "{\"name\": \"John Doe Batman\", \"age\": 18}"})
	store.DInsert([]string{"users", "{\"name\": \"Batman\", \"age\": 35}"})

	store.DQuery([]string{"users", "{\"name\": \"John Doe\"}"})
}
