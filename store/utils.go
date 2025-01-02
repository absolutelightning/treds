package store

import (
	"fmt"
	"reflect"
	"strings"

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
