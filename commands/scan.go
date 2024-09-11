package commands

import (
	"fmt"
	"math"
	"strconv"
	"treds/store"
)

const PrefixScanCommand = "SCAN"

func RegisterPrefixScan(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     PrefixScanCommand,
		Validate: validatePrefixScan(),
		Execute:  executePrefixScan(),
	})
}

func validatePrefixScan() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}
		if len(args) > 3 {
			return fmt.Errorf("expected maximum 3 argument, got %d", len(args))
		}
		return nil
	}
}

func executePrefixScan() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		count := strconv.Itoa(math.MaxInt64)
		if len(args) == 3 {
			count = args[2]
		}
		v, err := store.PrefixScan(args[0], args[1], count)
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
