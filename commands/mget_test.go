package commands

import (
	"testing"
)

// TestRegisterMGetCommand tests the RegisterMGetCommand function.
func TestRegisterMGetCommand(t *testing.T) {
	registry := NewRegistry()
	RegisterMGetCommand(registry)

	if _, exists := registry.(*commandRegistry).commands[MGetCommand]; !exists {
		t.Errorf("command %s not registered", MGetCommand)
	}
}

// TestValidateMGet tests the validateMGet function.
func TestValidateMGet(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectErr   bool
		expectedMsg string
	}{
		{"valid args", []string{"key1"}, false, ""},
		{"no args", []string{}, true, "expected atlest 1 argument, got 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validationHook := validateMGet()
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

// TestExecuteMGet tests the executeMGet function.
func TestExecuteMGet(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		store       *MockStore
		expectErr   bool
		expectedMsg string
	}{
		{
			name:        "get multiple existing keys",
			args:        []string{"key1", "key2"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2"}},
			expectErr:   false,
			expectedMsg: "value1\nvalue2\n",
		},
		{
			name:        "get existing and non-existent keys",
			args:        []string{"key1", "key3"},
			store:       &MockStore{data: map[string]string{"key1": "value1"}},
			expectErr:   false,
			expectedMsg: "value1\n(nil)\n",
		},
		{
			name:        "get non-existent keys",
			args:        []string{"key4"},
			store:       &MockStore{data: map[string]string{}},
			expectErr:   false,
			expectedMsg: "(nil)\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionHook := executeMGet()
			result := executionHook(tt.args, tt.store)
			if result != tt.expectedMsg {
				t.Errorf("expected result: %s, got: %s", tt.expectedMsg, result)
			}
		})
	}
}
