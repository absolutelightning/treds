package commands

import (
	"fmt"
	"math"
	"regexp"
	"strconv"

	"treds/store"
)

const KVSCommand = "KVS"

func RegisterKVSCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     KVSCommand,
		Validate: validateKVS(),
		Execute:  executeKVS(),
	})
}

func validateKVS() ValidationHook {
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
		_, err := regexp.Compile(args[0])
		if err != nil {
			return err
		}
		return nil
	}
}

func executeKVS() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		regex := ""
		count := math.MaxInt64
		if len(args) == 2 {
			regex = args[1]
		}
		if len(args) == 3 {
			count, _ = strconv.Atoi(args[2])
		}
		v, err := store.KVS(args[0], regex, count)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", v), nil
	}
}
