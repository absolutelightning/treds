package commands

import (
	"testing"
)

// TestRegisterKVSCommand tests the RegisterKVSCommand function.
func TestRegisterKVSCommand(t *testing.T) {
	registry := NewRegistry()
	RegisterKVSCommand(registry)

	if _, exists := registry.(*commandRegistry).commands[KVSCommand]; !exists {
		t.Errorf("command %s not registered", KVSCommand)
	}
}

// TestExecuteKVS tests the executeKVS function.
func TestExecuteKVS(t *testing.T) {
	t.Skip()
	tests := []struct {
		name        string
		args        []string
		store       *MockStore
		expectErr   bool
		expectedMsg string
	}{
		{
			name:        "retrieve all key-value pairs",
			args:        []string{"0"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2"}},
			expectErr:   false,
			expectedMsg: "key1\nvalue1\nkey2\nvalue2\n",
		},
		{
			name:        "retrieve key-value pairs with matching prefix",
			args:        []string{"0", "^key"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2", "other": "value3"}},
			expectErr:   false,
			expectedMsg: "key1\nvalue1\nkey2\nvalue2\n",
		},
		{
			name:        "no matching key-value pairs",
			args:        []string{"0", "nomatch"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2"}},
			expectErr:   false,
			expectedMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionHook := executeKVS()
			result := executionHook(tt.args, tt.store)
			if result != tt.expectedMsg {
				t.Errorf("expected result: %s, got: %s", tt.expectedMsg, result)
			}
		})
	}
}
