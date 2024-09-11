package commands

import (
	"fmt"
	"regexp"
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
		if len(args) != 1 {
			_, err := regexp.Compile(args[0])
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func executeKVS() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		regex := ""
		if len(args) == 1 {
			regex = args[0]
		}
		v, err := store.KVS(regex)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", v), nil
	}
}
