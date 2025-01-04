package store

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/absolutelightning/gods/maps/treemap"
	"github.com/tidwall/gjson"
	"treds/datastructures/radix"
)

const (
	TotalKeysExamined = "totalKeysExamined"
)

func validateDocument(collection *Collection, document *Document) error {
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

// lowerBoundIndex finds the smallest index where the key is greater than or equal to the target.
// It assumes `target` is of type `IndexValues`.
func lowerBoundIndex(tm *treemap.Map, target IndexValues) int {
	keys := tm.Keys() // Returns sorted keys based on the custom comparator
	return sort.Search(len(keys), func(i int) bool {
		key := keys[i].(IndexValues) // Type assertion to IndexValues
		return CustomComparator(key, target) >= 0
	})
}

// upperBoundIndex finds the smallest index where the key is strictly greater than the target.
// It assumes `target` is of type `IndexValues`.
func upperBoundIndex(tm *treemap.Map, target IndexValues) int {
	keys := tm.Keys() // Returns sorted keys based on the custom comparator
	return sort.Search(len(keys), func(i int) bool {
		key := keys[i].(IndexValues) // Type assertion to IndexValues
		return CustomComparator(key, target) > 0
	})
}

func estimateKeysExamined(filter QueryFilter, treeMap *treemap.Map) int {
	totalKeys := len(treeMap.Keys())
	if totalKeys == 0 {
		return 0 // Avoid division by zero
	}

	var lower, upper IndexValues
	minKey, _ := treeMap.Min()
	lower = minKey.(IndexValues)
	maxKey, _ := treeMap.Max()
	upper = maxKey.(IndexValues)

	switch filter.Operator {
	case "$gte":
		lower = IndexValues{FieldValues: []interface{}{filter.Value}}
	case "$gt":
		greaterKey, _ := treeMap.Greater(IndexValues{FieldValues: []interface{}{filter.Value}})
		if greaterKey != nil {
			lower = greaterKey.(IndexValues)
		} else {
			return 0
		}
	case "$lte":
		upper = IndexValues{FieldValues: []interface{}{filter.Value}}
	case "$lt":
		lowerKey, _ := treeMap.Lower(IndexValues{FieldValues: []interface{}{filter.Value}})
		if lowerKey != nil {
			upper = lowerKey.(IndexValues)
		} else {
			return 0
		}
	}

	startIndex := lowerBoundIndex(treeMap, lower)
	endIndex := upperBoundIndex(treeMap, upper)

	keysInRange := endIndex - startIndex
	if keysInRange < 0 {
		keysInRange = 0
	}

	return keysInRange
}

func executeQueryPlan(collection *Collection, query *Query) map[string]interface{} {
	// Retrieve the index to use

	executionPlanData := make(map[string]interface{})

	for indexName, index := range collection.Indices {
		// Estimate the cost of using the index
		cost := estimateEnhancedIndexCost(query, index)
		executionPlanData[indexName] = cost
	}

	return executionPlanData
}

func estimateEnhancedIndexCost(query *Query, index *Index) map[string]interface{} {
	cost := make(map[string]interface{})
	keysExamined := 0
	treeMap := index.indexer
	totalKeys := len(treeMap.Keys()) // Total number of keys in the index

	if totalKeys == 0 {
		cost[TotalKeysExamined] = keysExamined
		return cost // If the index is empty, cost is 0
	}

	// Calculate the cost of index scans
	scannedKeys := totalKeys
	for _, filter := range query.Filters {
		if canUseIndex(filter, index) {
			filteredKeys := estimateKeysExamined(filter, treeMap)
			scannedKeys = min(scannedKeys, filteredKeys) // Adjust based on filter
		}
	}

	keysExamined += scannedKeys // Add the cost of scanning the keys
	cost[TotalKeysExamined] = keysExamined

	return cost
}

func canUseIndex(filter QueryFilter, index *Index) bool {
	for _, field := range index.Fields.Fields {
		if filter.Field == field {
			return true
		}
	}
	return false
}

func fullScan(collection *Collection, query *Query) []*Document {
	filtered := make([]*Document, 0)

	// Iterate over all documents
	for _, doc := range collection.Documents {
		matches := true

		// Check if the document matches all query filters
		for _, filter := range query.Filters {
			if !matchesFilter(doc.StringData, filter) {
				matches = false
				break
			}
		}

		// Add the document if it matches all filters
		if matches {
			filtered = append(filtered, doc)
		}
	}

	// Apply sorting and pagination
	return applySortingAndPagination(filtered, query)
}

func applySortingAndPagination(results []*Document, query *Query) []*Document {
	// Apply sorting if specified
	if query.Sort != nil {
		sort.Slice(results, func(i, j int) bool {
			for _, criteria := range query.Sort {
				field := criteria.Field
				order := criteria.Order

				valI := results[i].Fields[field]
				valJ := results[j].Fields[field]

				// Compare values
				switch {
				case valI.(int) < valJ.(int):
					return order == "asc"
				case valI.(int) > valJ.(int):
					return order == "desc"
				}
				// If values are equal, move to the next criteria
			}
			return false // If all fields are equal, maintain original order
		})
	}

	// Apply limit and offset
	start := query.Offset
	end := start + query.Limit
	if end > len(results) {
		end = len(results)
	}

	return results[start:end]
}

func fetchAndFilterDocuments(collection *Collection, query *Query, index *Index) []*Document {
	// Get the treemap from the index
	treeMap := index.indexer
	totalKeys := len(treeMap.Keys())
	if totalKeys == 0 {
		return nil // Return an empty result if index is empty
	}

	documentIds := make(map[string]struct{})

	processFilter := func(filter QueryFilter) map[string]struct{} {
		matchingIds := make(map[string]struct{})

		// Estimate the range of keys to examine based on the filter
		var lower, upper IndexValues
		minKey, _ := treeMap.Min()
		lower = minKey.(IndexValues)
		maxKey, _ := treeMap.Max()
		upper = maxKey.(IndexValues)

		switch filter.Operator {
		case "$gte":
			lower = IndexValues{FieldValues: []interface{}{filter.Value}}
		case "$gt":
			greaterKey, _ := treeMap.Greater(IndexValues{FieldValues: []interface{}{filter.Value}})
			if greaterKey != nil {
				lower = greaterKey.(IndexValues)
			} else {
				return matchingIds
			}
		case "$lte":
			upper = IndexValues{FieldValues: []interface{}{filter.Value}}
		case "$lt":
			lowerKey, _ := treeMap.Lower(IndexValues{FieldValues: []interface{}{filter.Value}})
			if lowerKey != nil {
				upper = lowerKey.(IndexValues)
			} else {
				return matchingIds
			}
		}

		// Fetch keys in the range
		startIndex := lowerBoundIndex(treeMap, lower)
		endIndex := upperBoundIndex(treeMap, upper)
		keysInRange := treeMap.Keys()[startIndex:endIndex]

		// Collect matching document IDs
		for _, key := range keysInRange {
			startRadixTree, _ := treeMap.Get(key)
			leaf, found := startRadixTree.(*radix.Tree).Root().MinimumLeaf()
			if !found {
				continue
			}
			for leaf != nil {
				matchingIds[string(leaf.Key())] = struct{}{}
				leaf = leaf.GetNextLeaf()
				if leaf != nil {
					nextLeafValue := leaf.Value().(IndexValues)
					if CustomComparator(nextLeafValue, upper) > 0 {
						break
					}
				}
			}
		}

		return matchingIds
	}

	processLogicalFilter := func(filter QueryFilter) {
		switch filter.Logical {
		case "$and":
			// Start with all document IDs and filter progressively
			for i, subFilter := range filter.SubFilters {
				subIds := processFilter(subFilter)
				if i == 0 {
					documentIds = subIds // Initialize with the first subfilter result
				} else {
					for id := range documentIds {
						if _, exists := subIds[id]; !exists {
							delete(documentIds, id) // Remove IDs that don't match the subfilter
						}
					}
				}
			}
		case "$or":
			// Collect IDs matching any subfilter
			for _, subFilter := range filter.SubFilters {
				subIds := processFilter(subFilter)
				for id := range subIds {
					documentIds[id] = struct{}{}
				}
			}
		case "$not":
			// Remove IDs matching the subfilters
			for _, subFilter := range filter.SubFilters {
				subIds := processFilter(subFilter)
				for id := range subIds {
					delete(documentIds, id)
				}
			}
		}
	}

	// Process all filters
	for _, filter := range query.Filters {
		if filter.Logical != "" {
			processLogicalFilter(filter)
		} else {
			matchingIds := processFilter(filter)
			for id := range matchingIds {
				documentIds[id] = struct{}{}
			}
		}
	}

	// Filter and fetch the final documents
	var result []*Document
	for docId := range documentIds {
		doc := collection.Documents[docId]
		if matchesFilters(doc.StringData, query.Filters) { // Pass actual filters
			result = append(result, doc)
		}
	}

	return result
}

func matchesFilters(jsonDoc string, filters []QueryFilter) bool {
	for _, filter := range filters {
		if filter.Logical == "$and" {
			for _, subFilter := range filter.SubFilters {
				if !matchesFilter(jsonDoc, subFilter) {
					return false
				}
			}
		} else if filter.Logical == "$or" {
			matched := false
			for _, subFilter := range filter.SubFilters {
				if matchesFilter(jsonDoc, subFilter) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		} else if filter.Logical == "$not" {
			for _, subFilter := range filter.SubFilters {
				if matchesFilter(jsonDoc, subFilter) {
					return false
				}
			}
		} else {
			if !matchesFilter(jsonDoc, filter) {
				return false
			}
		}
	}
	return true
}

func matchesFilter(jsonDoc string, filter QueryFilter) bool {
	if filter.Logical != "" {
		switch filter.Logical {
		case "$and":
			for _, subFilter := range filter.SubFilters {
				if !matchesFilter(jsonDoc, subFilter) {
					return false
				}
			}
			return true
		case "$or":
			for _, subFilter := range filter.SubFilters {
				if matchesFilter(jsonDoc, subFilter) {
					return true
				}
			}
			return false
		case "$not":
			for _, subFilter := range filter.SubFilters {
				if matchesFilter(jsonDoc, subFilter) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}

	// Process non-logical operators
	result := gjson.Get(jsonDoc, filter.Field)
	if !result.Exists() {
		return false
	}

	switch filter.Operator {
	case "$eq":
		switch result.Type {
		case gjson.String:
			return result.String() == filter.Value.(string)
		case gjson.Number:
			return result.Float() == filter.Value.(float64)
		case gjson.True, gjson.False:
			return result.Bool() == filter.Value.(bool)
		default:
			panic("unhandled default case")
		}
	case "$gt":
		switch result.Type {
		case gjson.Number:
			return result.Float() > filter.Value.(float64)
		case gjson.String:
			return result.String() > filter.Value.(string)
		default:
			panic("unhandled default case")
		}
	case "$gte":
		switch result.Type {
		case gjson.Number:
			return result.Float() >= filter.Value.(float64)
		case gjson.String:
			return result.String() >= filter.Value.(string)
		default:
			panic("unhandled default case")
		}
	case "$lt":
		switch result.Type {
		case gjson.Number:
			return result.Float() < filter.Value.(float64)
		case gjson.String:
			return result.String() < filter.Value.(string)
		default:
			panic("unhandled default case")
		}
	case "$lte":
		switch result.Type {
		case gjson.Number:
			return result.Float() <= filter.Value.(float64)
		case gjson.String:
			return result.String() <= filter.Value.(string)
		default:
			panic("unhandled default case")
		}
	}
	return false
}
