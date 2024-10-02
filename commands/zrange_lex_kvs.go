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
		if len(args) > 6 {
			return fmt.Errorf("expected maximum 3 argument, got %d", len(args))
		}
		return nil
	}
}

func executeZRangeLex() ExecutionHook {
	return func(args []string, store store.Store) string {
		count := strconv.Itoa(math.MaxInt64)
		if len(args) > 2 {
			count = args[2]
		}
		withScore := true
		if len(args) > 3 {
			withScore, _ = strconv.ParseBool(args[3])
		}
		minKey := ""
		if len(args) > 4 {
			minKey = args[4]
		}
		maxKey := ""
		if len(args) > 5 {
			maxKey = args[5]
		}
		v, err := store.ZRangeByLexKVS(args[0], args[1], minKey, maxKey, count, withScore)
		if err != nil {
			return err.Error()
		}
		return v
	}
}
