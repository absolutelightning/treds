package commands

import (
	"testing"
)

// TestRegisterKeysCommand tests the RegisterKeysCommand function.
func TestRegisterKeysCommand(t *testing.T) {
	registry := NewRegistry()
	RegisterKeysCommand(registry)

	if _, exists := registry.(*commandRegistry).commands[KeysCommand]; !exists {
		t.Errorf("command %s not registered", KeysCommand)
	}
}

// TestExecuteKeys tests the executeKeys function.
func TestExecuteKeys(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		store       *MockStore
		expectErr   bool
		expectedMsg string
	}{
		{
			name:        "retrieve all keys",
			args:        []string{"0"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2"}},
			expectErr:   false,
			expectedMsg: "key1\nkey2\n",
		},
		{
			name:        "retrieve keys with matching prefix",
			args:        []string{"0", "^key"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2", "other": "value3"}},
			expectErr:   false,
			expectedMsg: "key1\nkey2\n",
		},
		{
			name:        "no matching keys",
			args:        []string{"0", "nomatch"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2"}},
			expectErr:   false,
			expectedMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionHook := executeKeys()
			result, err := executionHook(tt.args, tt.store)
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error: %v, got: %v", tt.expectErr, err)
			}
			if err == nil && result != tt.expectedMsg {
				t.Errorf("expected result: %s, got: %s", tt.expectedMsg, result)
			} else if err != nil && err.Error() != tt.expectedMsg {
				t.Errorf("expected error message: %s, got: %s", tt.expectedMsg, err.Error())
			}
		})
	}
}
