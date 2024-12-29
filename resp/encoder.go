package resp

import (
	"bytes"
	"fmt"
)

// EncodeSimpleString encodes a RESP simple string
func EncodeSimpleString(s string) string {
	return "+" + s + "\r\n"
}

// EncodeBulkString encodes a RESP bulk string, handling null values
func EncodeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
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
