package commands

import (
	"testing"
)

// TestRegisterSetCommand tests the RegisterSetCommand function.
func TestRegisterSetCommand(t *testing.T) {
	registry := NewRegistry()
	RegisterSetCommand(registry)

	if _, exists := registry.(*commandRegistry).commands[SetCommand]; !exists {
		t.Errorf("command %s not registered", SetCommand)
	}
}

// TestValidateSet tests the validateSet function.
func TestValidateSet(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectErr   bool
		expectedMsg string
	}{
		{"valid args", []string{"key1", "value1"}, false, ""},
		{"too few args", []string{"key1"}, true, "expected 2 argument, got 1"},
		{"too many args", []string{"key1", "value1", "extra"}, true, "expected 2 argument, got 3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validationHook := validateSet()
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

// TestExecuteSet tests the executeSet function.
func TestExecuteSet(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		store       *MockStore
		expectErr   bool
		expectedMsg string
	}{
		{
			name:        "set key-value pair",
			args:        []string{"key1", "value1"},
			store:       &MockStore{data: make(map[string]string)},
			expectErr:   false,
			expectedMsg: "OK\n",
		},
		{
			name:        "overwrite existing key",
			args:        []string{"key1", "newvalue"},
			store:       &MockStore{data: map[string]string{"key1": "value1"}},
			expectErr:   false,
			expectedMsg: "OK\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionHook := executeSet()
			result, err := executionHook(tt.args, tt.store)
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error: %v, got: %v", tt.expectErr, err)
			}
			if err == nil && result != tt.expectedMsg {
				t.Errorf("expected result: %s, got: %s", tt.expectedMsg, result)
			}

			// Verify the set operation
			if value, exists := tt.store.data[tt.args[0]]; !exists || value != tt.args[1] {
				t.Errorf("expected store to contain key %s with value %s, but got %s", tt.args[0], tt.args[1], value)
			}
		})
	}
}
