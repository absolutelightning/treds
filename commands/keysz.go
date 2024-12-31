package commands

import (
	"fmt"
	"math"
	"regexp"
	"strconv"

	"treds/resp"
	"treds/store"
)

const KeysZCommand = "KEYSZ"

func RegisterKeysZCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     KeysHCommand,
		Validate: validateKeysZ(),
		Execute:  executeKeysZ(),
	})
}

func validateKeysZ() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}
		if len(args) == 3 {
			_, err := strconv.Atoi(args[2])
			if err != nil {
				return err
			}
		}
		_, err := regexp.Compile(args[1])
		if err != nil {
			return err
		}
		return nil
	}
}

func executeKeysZ() ExecutionHook {
	return func(args []string, store store.Store) string {
		regex := ""
		count := math.MaxInt64
		if len(args) >= 2 {
			regex = args[1]
		}
		if len(args) == 3 {
			count, _ = strconv.Atoi(args[2])
		}
		v, err := store.KeysZ(args[0], regex, count)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.EncodeStringArray(v)
	}
}
