package commands

import (
	"fmt"
	"math"
	"strconv"
	"treds/store"
)

const ZRANGELEXKVS = "ZRANGELEXKVS"

func RegisterZRangeLexCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZRANGELEXKVS,
		Validate: validateZRangeLex(),
		Execute:  executeZRangeLex(),
	})
}

func validateZRangeLex() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}
		if len(args) > 4 {
			return fmt.Errorf("expected maximum 3 argument, got %d", len(args))
		}
		return nil
	}
}

func executeZRangeLex() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		count := strconv.Itoa(math.MaxInt64)
		if len(args) > 3 {
			count = args[3]
		}
		prefix := ""
		if len(args) > 2 {
			prefix = args[2]
		}
		v, err := store.ZRangeByLexKVS(args[0], args[1], prefix, count)
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
