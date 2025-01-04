package main

import (
	"fmt"

	"treds/store"
)

func main() {
	store := store.NewTredsStore()
	err := store.DCreateCollection([]string{"users", "{\"name\": {\"type\": \"string\"}, \"age\": {\"type\": \"float\", \"min\": 18}, \"salary\": {\"type\": \"float\"}}", "[{\"fields\": [\"salary\"], \"type\": \"normal\"}, {\"fields\": [\"age\"], \"type\": \"normal\"}]"})
	if err != nil {
		fmt.Println(err)
	}
	store.DInsert([]string{"users", "{\"name\": \"Spiderman\", \"age\": 13, \"salary\": 500}"})
	store.DInsert([]string{"users", "{\"name\": \"Heman\", \"age\": 14, \"salary\": 600}"})
	store.DInsert([]string{"users", "{\"name\": \"Superman\", \"age\": 15, \"salary\": 300}"})
	store.DInsert([]string{"users", "{\"name\": \"Batman\", \"age\": 18, \"salary\": 700}"})
	store.DInsert([]string{"users", "{\"name\": \"Flash\", \"age\": 35, \"salary\": 800}"})

	plan, _ := store.DExecutionPlan([]string{"users", "{\"filters\":[{\"field\":\"age\",\"operator\":\"$gt\",\"value\":14},{\"field\":\"salary\",\"operator\":\"$lt\",\"value\":900}]}"})
	fmt.Println(plan)
}
