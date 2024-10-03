package commands

import (
	"testing"
)

// TestRegisterDeletePrefixCommand tests the RegisterDeletePrefixCommand function.
func TestRegisterDeletePrefixCommand(t *testing.T) {
	registry := NewRegistry()
	RegisterDeletePrefixCommand(registry)

	if _, exists := registry.(*commandRegistry).commands[DeletePrefixCommand]; !exists {
		t.Errorf("command %s not registered", DeletePrefixCommand)
	}
}

// TestValidateDeletePrefix tests the validateDeletePrefix function.
func TestValidateDeletePrefix(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectErr   bool
		expectedMsg string
	}{
		{"valid args", []string{"prefix1"}, false, ""},
		{"no args", []string{}, true, "expected 1 argument, got 0"},
		{"too many args", []string{"prefix1", "extra"}, true, "expected 1 argument, got 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validationHook := validateDeletePrefix()
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

// TestExecuteDeletePrefix tests the executeDeletePrefix function.
func TestExecuteDeletePrefix(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		store       *MockStore
		expectErr   bool
		expectedMsg string
	}{
		{"delete existing prefix", []string{"prefix"}, &MockStore{data: map[string]string{"prefix1": "", "prefix2": "", "other": ""}}, false, "0"},
		{"delete non-existent prefix", []string{"nonexistent"}, &MockStore{data: map[string]string{"key1": "", "key2": ""}}, false, "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionHook := executeDeletePrefix()
			result := executionHook(tt.args, tt.store)
			if result != tt.expectedMsg {
				t.Errorf("expected result: %s, got: %s", tt.expectedMsg, result)
			}
		})
	}
}
