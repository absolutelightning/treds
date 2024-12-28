package resp

import (
	"fmt"
	"strconv"
	"strings"
)

// Decode parses a RESP command string and returns the command and arguments
func Decode(respInput string) (string, []string, error) {
	lines := strings.Split(respInput, "\r\n")
	if len(lines) < 1 || lines[0][0] != '*' {
		return "", nil, fmt.Errorf("invalid RESP input: missing array prefix '*'")
	}

	// Parse the array length
	arrayLength, err := strconv.Atoi(lines[0][1:])
	if err != nil {
		return "", nil, fmt.Errorf("invalid array length: %v", err)
	}

	if arrayLength < 1 {
		return "", nil, fmt.Errorf("invalid command: array length must be at least 1")
	}

	// Parse the bulk strings
	args := make([]string, 0, arrayLength)
	i := 1
	for len(args) < arrayLength && i < len(lines) {
		if len(lines[i]) > 0 && lines[i][0] == '$' {
			// Parse the bulk string length
			bulkLength, err := strconv.Atoi(lines[i][1:])
			if err != nil || bulkLength < 0 {
				return "", nil, fmt.Errorf("invalid bulk string length: %v", err)
			}

			// Ensure the bulk string value exists
			if i+1 >= len(lines) || len(lines[i+1]) != bulkLength {
				return "", nil, fmt.Errorf("bulk string length mismatch")
			}

			// Append the value to args
			args = append(args, lines[i+1])
			i += 2 // Move to the next bulk string header
		} else {
			return "", nil, fmt.Errorf("expected bulk string prefix '$', found: %s", lines[i])
		}
	}

	if len(args) != arrayLength {
		return "", nil, fmt.Errorf("mismatch between declared and parsed array length")
	}

	// The first argument is the command (e.g., "SET")
	command := args[0]
	arguments := args[1:] // The rest are the actual arguments

	return command, arguments, nil
}
