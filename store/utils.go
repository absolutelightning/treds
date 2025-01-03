package store

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/tidwall/gjson"
)

func ValidateDocument(collection *Collection, document *Document) error {
	for field, value := range document.Fields {
		// Check if the field exists in the schema
		schemaType, found := collection.Schema[field]
		if !found {
			return fmt.Errorf("field %s not found in schema", field)
		}

		// Normalize schema type to lower case for case-insensitive comparison
		schemaTypeVal := strings.ToLower(schemaType.(map[string]interface{})["type"].(string))

		// Get the expected type from the schema
		expectedType, exists := TypeMapping[schemaTypeVal]
		if !exists {
			return fmt.Errorf("unsupported type %s for field %s", schemaType, field)
		}

		// Check if the value's type matches the expected type
		if reflect.TypeOf(value) != expectedType {
			return fmt.Errorf("field %s has invalid type: expected %s, got %s", field, expectedType, reflect.TypeOf(value).String())
		}
	}
	return nil
}

// getValue dynamically returns the appropriate type from a gjson.Result
func getValue(result gjson.Result) interface{} {
	switch result.Type {
	case gjson.Number:
		return result.Float() // Return float64 for numbers
	case gjson.String:
		return result.String() // Return string
	case gjson.True, gjson.False:
		return result.Bool() // Return bool
	case gjson.Null:
		return nil // Return nil for null or missing values
	case gjson.JSON:
		return result.Raw // Return raw JSON for objects/arrays as string
	default:
		return nil // Return nil for unknown types
	}
}

func CustomComparator(a, b interface{}) int {
	// Type assertion for IndexValues
	keyA := a.(IndexValues)
	keyB := b.(IndexValues)

	// Determine the minimum length of FieldValues
	minLength := len(keyA.FieldValues)
	if len(keyB.FieldValues) < minLength {
		minLength = len(keyB.FieldValues)
	}

	// Compare each field sequentially
	for i := 0; i < minLength; i++ {
		switch valA := keyA.FieldValues[i].(type) {
		case int:
			valB := keyB.FieldValues[i].(int)
			switch {
			case valA < valB:
				return -1
			case valA > valB:
				return 1
			}
		case string:
			valB := keyB.FieldValues[i].(string)
			switch {
			case valA < valB:
				return -1
			case valA > valB:
				return 1
			}
		case float64:
			valB := keyB.FieldValues[i].(float64)
			switch {
			case valA < valB:
				return -1
			case valA > valB:
				return 1
			}
		default:
			panic("Unsupported type in FieldValues")
		}
	}

	// If all compared fields are equal, compare lengths
	switch {
	case len(keyA.FieldValues) < len(keyB.FieldValues):
		return -1
	case len(keyA.FieldValues) > len(keyB.FieldValues):
		return 1
	}

	// Keys are equal
	return 0
}

// LowerBoundIndex finds the smallest index where the key is greater than or equal to the target.
// It assumes `target` is of type `IndexValues`.
func LowerBoundIndex(tm *treemap.Map, target *IndexValues) int {
	keys := tm.Keys() // Returns sorted keys based on the custom comparator
	return sort.Search(len(keys), func(i int) bool {
		key := keys[i].(IndexValues) // Type assertion to IndexValues
		return CustomComparator(key, target) >= 0
	})
}

// UpperBoundIndex finds the smallest index where the key is strictly greater than the target.
// It assumes `target` is of type `IndexValues`.
func UpperBoundIndex(tm *treemap.Map, target *IndexValues) int {
	keys := tm.Keys() // Returns sorted keys based on the custom comparator
	return sort.Search(len(keys), func(i int) bool {
		key := keys[i].(IndexValues) // Type assertion to IndexValues
		return CustomComparator(key, target) > 0
	})
}

func estimateRangeFraction(filter QueryFilter, treeMap *treemap.Map) float64 {
	totalKeys := len(treeMap.Keys())
	if totalKeys == 0 {
		return 0.0 // Avoid division by zero
	}

	var lower, upper *IndexValues
	switch filter.Operator {
	case "$gt", "$gte":
		lower = &IndexValues{FieldValues: []interface{}{filter.Value}}
	case "$lt", "$lte":
		upper = &IndexValues{FieldValues: []interface{}{filter.Value}}
	}

	startIndex := 0
	if lower != nil {
		startIndex = LowerBoundIndex(treeMap, lower)
	}

	endIndex := totalKeys
	if upper != nil {
		endIndex = UpperBoundIndex(treeMap, upper)
	}

	keysInRange := endIndex - startIndex
	if keysInRange < 0 {
		keysInRange = 0
	}

	return float64(keysInRange) / float64(totalKeys)
}

func ExecuteQueryPlan(collection *Collection, query *QueryPlan) map[string]interface{} {
	// Retrieve the index to use

	executionPlanData := make(map[string]interface{})

	for indexName, index := range collection.Indices {
		// Estimate the cost of using the index
		cost := estimateEnhancedIndexCost(query, index)
		executionPlanData[indexName] = cost
	}

	return executionPlanData
}

func estimateEnhancedIndexCost(query *QueryPlan, index *Index) int {
	cost := 0
	treeMap := index.indexer
	totalKeys := len(treeMap.Keys()) // Total number of keys in the index

	if totalKeys == 0 {
		return cost // If the index is empty, cost is 0
	}

	// Calculate the cost of index scans
	scannedKeys := totalKeys
	for _, filter := range query.Filters {
		if canUseIndex(filter, index) {
			rangeFraction := estimateRangeFraction(filter, treeMap)
			filteredKeys := int(rangeFraction * float64(scannedKeys))
			scannedKeys = min(scannedKeys, filteredKeys) // Adjust based on filter
		} else {
			scannedKeys = totalKeys // If filter cannot use the index, assume full scan
		}
	}

	cost += scannedKeys // Add the cost of scanning the keys

	// Estimate document fetch cost
	docFetchCost := estimateDocsFetched(query, index)
	cost += docFetchCost

	return cost
}

func estimateDocsFetched(query *QueryPlan, index *Index) int {
	keys := index.indexer.Keys() // Retrieve all keys from the index
	totalDocs := len(keys)
	if totalDocs == 0 {
		return 0 // No documents to fetch
	}

	// Calculate the intersection of all filters
	docsFetched := totalDocs
	for _, filter := range query.Filters {
		rangeFraction := estimateRangeFraction(filter, index.indexer)
		docsFetched = int(rangeFraction * float64(docsFetched))
		if docsFetched == 0 {
			break // Short-circuit if no documents are matched
		}
	}

	return docsFetched
}

func canUseIndex(filter QueryFilter, index *Index) bool {
	for _, field := range index.Fields.Fields {
		if filter.Field == field {
			return true
		}
	}
	return false
}
