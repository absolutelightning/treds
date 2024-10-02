package store

import (
	"errors"
	"strings"
	"unicode"
)

const maxKeyLength = 512 * 1024 * 1024 // 512 MB

func isBalanced(openStack []rune, char rune) bool {
	if len(openStack) == 0 {
		return false
	}
	last := openStack[len(openStack)-1]
	switch char {
	case ')':
		return last == '('
	case '}':
		return last == '{'
	case ']':
		return last == '['
	}
	return false
}

func validateKey(key string) bool {
	// Check if key exceeds maximum allowed length
	if len(key) > maxKeyLength {
		return false
	}

	// Disallow keys that look like JSON-like structures (i.e., they contain unbalanced curly braces and quotes)
	if isJSONLikeKey(key) {
		return false
	}

	// Iterate through each character to check for control or non-printable characters
	for _, char := range key {
		if unicode.IsControl(char) || !unicode.IsPrint(char) {
			return false
		}
	}
	return true
}

func isJSONLikeKey(key string) bool {
	openBraces := 0
	inDoubleQuotes := false

	for _, char := range key {
		switch char {
		case '{':
			openBraces++
		case '}':
			openBraces--
			if openBraces < 0 {
				return true // More closing braces than opening braces
			}
		case '"':
			inDoubleQuotes = !inDoubleQuotes
		}
	}

	// Return true if unbalanced braces or unbalanced quotes
	if openBraces != 0 || inDoubleQuotes {
		return true
	}

	return false
}

// Function to split command while respecting quotes, parentheses, brackets, and braces
func splitCommandWithQuotes(command string) ([]string, error) {
	var result []string
	var current strings.Builder
	var openStack []rune

	inDoubleQuotes := false
	inSingleQuotes := false

	for _, char := range command {
		switch char {
		case '"':
			if !inSingleQuotes && len(openStack) == 0 { // Only toggle double quotes if not inside single quotes or brackets
				inDoubleQuotes = !inDoubleQuotes
			}
			current.WriteRune(char)
		case '\'':
			if !inDoubleQuotes && len(openStack) == 0 { // Only toggle single quotes if not inside double quotes or brackets
				inSingleQuotes = !inSingleQuotes
			}
			current.WriteRune(char)
		case '(', '[', '{':
			if !inSingleQuotes && !inDoubleQuotes { // Track opening brackets
				openStack = append(openStack, char)
			}
			current.WriteRune(char)
		case ')', ']', '}':
			if !inSingleQuotes && !inDoubleQuotes {
				if isBalanced(openStack, char) {
					openStack = openStack[:len(openStack)-1] // Pop the last opened bracket
				} else {
					return nil, errors.New("unbalanced brackets")
				}
			}
			current.WriteRune(char)
		case ' ':
			if inDoubleQuotes || inSingleQuotes || len(openStack) > 0 {
				// Inside quotes or brackets, keep spaces as part of the value
				current.WriteRune(char)
			} else {
				// End of a token (not inside any quotes or brackets), add to result
				if current.Len() > 0 {
					result = append(result, current.String())
					current.Reset()
				}
			}
		default:
			current.WriteRune(char)
		}
	}
	// Check for unbalanced quotes or brackets
	if inDoubleQuotes || inSingleQuotes || len(openStack) > 0 {
		return nil, errors.New("unbalanced quotes or brackets")
	}
	// Add the last token to the result if it exists
	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result, nil
}
