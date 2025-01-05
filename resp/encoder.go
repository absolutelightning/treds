package resp

import (
	"bytes"
	"fmt"
	"strconv"
)

// EncodeSimpleString encodes a RESP simple string
func EncodeSimpleString(s string) string {
	return "+" + s + "\r\n"
}

// EncodeBulkString encodes a RESP bulk string, handling null values
func EncodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// Encode2DStringArrayRESP takes a 2D slice of strings and encodes
// it in the Redis Serialization Protocol (RESP) format.
func Encode2DStringArrayRESP(arr [][]string) string {
	// Outer array length
	output := "*" + strconv.Itoa(len(arr)) + "\r\n"

	// For each sub-slice
	for _, sub := range arr {
		// 1) Mark this sub-slice as a RESP Array
		output += "*" + strconv.Itoa(len(sub)) + "\r\n"

		// 2) For each string in sub-slice
		for _, s := range sub {
			// Bulk string: length, then content
			output += "$" + strconv.Itoa(len(s)) + "\r\n" +
				s + "\r\n"
		}
	}
	return output
}

// EncodeError encodes a RESP error string
func EncodeError(err string) string {
	return "-" + err + "\r\n"
}

// EncodeInteger encodes a RESP integer
func EncodeInteger(i int) string {
	return fmt.Sprintf(":%d\r\n", i)
}

// EncodeStringArray encodes an array of strings into RESP array format
func EncodeStringArray(arr []string) string {
	var buffer bytes.Buffer

	// Write the array header (*<number_of_elements>\r\n)
	buffer.WriteString(fmt.Sprintf("*%d\r\n", len(arr)))

	// Encode each string as a bulk string and append to the buffer
	for _, s := range arr {
		buffer.WriteString(EncodeBulkString(s))
	}

	return buffer.String()
}

// EncodeStringArray encodes an array of strings into RESP array format
func EncodeStringArrayRESP(arr []string) string {
	var buffer bytes.Buffer

	// Write the array header (*<number_of_elements>\r\n)
	buffer.WriteString(fmt.Sprintf("*%d\r\n", len(arr)))

	// Encode each string as a bulk string and append to the buffer
	for _, s := range arr {
		buffer.WriteString(s)
	}

	return buffer.String()
}

// EncodeArray encodes a RESP array, handling nested arrays and nulls
func EncodeArray(elements []interface{}) string {
	if elements == nil {
		return "*-1\r\n" // Null array
	}
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("*%d\r\n", len(elements)))
	for _, element := range elements {
		switch v := element.(type) {
		case string:
			buffer.WriteString(EncodeBulkString(v))
		case int:
			buffer.WriteString(EncodeInteger(v))
		case []interface{}:
			buffer.WriteString(EncodeArray(v))
		default:
			buffer.WriteString(EncodeError("ERR unsupported type"))
		}
	}
	return buffer.String()
}

// EncodeMap encodes a Go map as a RESP array of alternating keys and values
func EncodeMap(m map[string]string) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("*%d\r\n", len(m)*2)) // Each key-value pair is 2 elements
	for k, v := range m {
		buffer.WriteString(EncodeBulkString(k))
		buffer.WriteString(EncodeBulkString(v))
	}
	return buffer.String()
}
