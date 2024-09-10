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

// TestValidateKVS tests the validateKVS function.
func TestValidateKVS(t *testing.T) {
	validationHook := validateKVS()

	// The validateKVS function always returns nil, so we simply check that it does.
	if err := validationHook([]string{}); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}

	if err := validationHook([]string{"someArg"}); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

// TestExecuteKVS tests the executeKVS function.
func TestExecuteKVS(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		store       *MockStore
		expectErr   bool
		expectedMsg string
	}{
		{
			name:        "retrieve all key-value pairs",
			args:        []string{},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2"}},
			expectErr:   false,
			expectedMsg: "key1\nvalue1\nkey2\nvalue2\n",
		},
		{
			name:        "retrieve key-value pairs with matching prefix",
			args:        []string{"key"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2", "other": "value3"}},
			expectErr:   false,
			expectedMsg: "key1\nvalue1\nkey2\nvalue2\n",
		},
		{
			name:        "no matching key-value pairs",
			args:        []string{"nomatch"},
			store:       &MockStore{data: map[string]string{"key1": "value1", "key2": "value2"}},
			expectErr:   false,
			expectedMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionHook := executeKVS()
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
