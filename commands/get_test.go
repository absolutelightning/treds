package commands

import (
	"testing"
)

// TestRegisterGetCommand tests the RegisterGetCommand function.
func TestRegisterGetCommand(t *testing.T) {
	registry := NewRegistry()
	RegisterGetCommand(registry)

	if _, exists := registry.(*commandRegistry).commands[GetCommand]; !exists {
		t.Errorf("command %s not registered", GetCommand)
	}
}

// TestValidateGet tests the validateGet function.
func TestValidateGet(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectErr   bool
		expectedMsg string
	}{
		{"valid args", []string{"key1"}, false, ""},
		{"no args", []string{}, true, "expected 1 argument, got 0"},
		{"too many args", []string{"key1", "key2"}, true, "expected 1 argument, got 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validationHook := validateGet()
			err := validationHook(tt.args)
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error: %v, got: %v", tt.expectErr, err)
			}
			if err != nil && err.Error() != tt.expectedMsg {
				t.Errorf("expected error message: %s, got: %s", tt.expectedMsg, err.Error())
			}
		})
	}
}

// TestExecuteGet tests the executeGet function.
func TestExecuteGet(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		store       *MockStore
		expectErr   bool
		expectedMsg string
	}{
		{"get existing key", []string{"key1"}, &MockStore{data: map[string]string{"key1": "value1"}}, false, "value1"},
		{"get non-existent key", []string{"key2"}, &MockStore{data: map[string]string{}}, true, "key does not exist"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionHook := executeGet()
			result := executionHook(tt.args, tt.store)
			if result != tt.expectedMsg {
				t.Errorf("expected result: %s, got: %s", tt.expectedMsg, result)
			}
		})
	}
}
