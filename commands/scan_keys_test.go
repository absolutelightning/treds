package commands

import "testing"

// TestRegisterPrefixScanKeys tests the RegisterScanKeysCommand function.
func TestRegisterPrefixScanKeys(t *testing.T) {
	registry := NewRegistry()
	RegisterScanKeysCommand(registry)

	if _, exists := registry.(*commandRegistry).commands[PrefixScanKeysCommand]; !exists {
		t.Errorf("command %s not registered", PrefixScanKeysCommand)
	}
}

// TestValidatePrefixScanKeys tests the validatePrefixScanKeys function.
func TestValidatePrefixScanKeys(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		expectErr   bool
		expectedMsg string
	}{
		{"valid args with 2", []string{"0", "prefix"}, false, ""},
		{"valid args with 3", []string{"0", "prefix", "10"}, false, ""},
		{"no args", []string{}, true, "expected minimum 2 argument, got 0"},
		{"only 1 arg", []string{"prefix"}, true, "expected minimum 2 argument, got 1"},
		{"too many args", []string{"0", "prefix", "10", "extra"}, true, "expected maximum 3 argument, got 4"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validationHook := validatePrefixScanKeys()
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

// TestExecutePrefixScanKeys tests the executePrefixScanKeys function.
func TestExecutePrefixScanKeys(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		store       *MockStore
		expectErr   bool
		expectedMsg string
	}{
		{
			name:        "retrieve all keys with prefix",
			args:        []string{"0", "prefix"},
			store:       &MockStore{data: map[string]string{"prefix1": "value1", "prefix2": "value2", "other": "value3"}},
			expectErr:   false,
			expectedMsg: "prefix1\nprefix2\n",
		},
		{
			name:        "retrieve keys with prefix and limit count",
			args:        []string{"0", "prefix", "1"},
			store:       &MockStore{data: map[string]string{"prefix1": "value1", "prefix2": "value2", "other": "value3"}},
			expectErr:   false,
			expectedMsg: "prefix1\n",
		},
		{
			name:        "no matching keys with prefix",
			args:        []string{"0", "nomatch"},
			store:       &MockStore{data: map[string]string{"prefix1": "value1", "prefix2": "value2"}},
			expectErr:   false,
			expectedMsg: "",
		},
		{
			name:        "retrieve keys with cursor offset",
			args:        []string{"1", "prefix"},
			store:       &MockStore{data: map[string]string{"prefix1": "value1", "prefix2": "value2", "other": "value3"}},
			expectErr:   false,
			expectedMsg: "prefix2\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executionHook := executePrefixScanKeys()
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
