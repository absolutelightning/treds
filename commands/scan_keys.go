package commands

import (
	"fmt"
	"math"
	"strconv"

	"treds/resp"
	"treds/store"
)

const PrefixScanKeysCommand = "SCANKEYS"

func RegisterScanKeysCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     PrefixScanKeysCommand,
		Validate: validatePrefixScanKeys(),
		Execute:  executePrefixScanKeys(),
	})
}

func validatePrefixScanKeys() ValidationHook {
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

func executePrefixScanKeys() ExecutionHook {
	return func(args []string, store store.Store) string {
		count := strconv.Itoa(math.MaxInt64)
		if len(args) == 3 {
			count = args[2]
		}
		v, err := store.PrefixScanKeys(args[0], args[1], count)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeStringArray(v)
	}
}
