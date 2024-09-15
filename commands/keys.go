package commands

import (
	"fmt"
	"regexp"

	"treds/store"
)

const KeysCommand = "KEYS"

func RegisterKeysCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     KeysCommand,
		Validate: validateKeys(),
		Execute:  executeKeys(),
	})
}

func validateKeys() ValidationHook {
	return func(args []string) error {
		if len(args) == 1 {
			_, err := regexp.Compile(args[0])
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func executeKeys() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		regex := ""
		if len(args) == 1 {
			regex = args[0]
		}
		v, err := store.Keys(regex)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", v), nil
	}
}
