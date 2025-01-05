package commands

import (
	"fmt"

	"treds/resp"
	"treds/store"
)

const VSearch = "VSEARCH"

func RegisterVSearch(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     VSearch,
		Validate: validateVSearch(),
		Execute:  executeVSearch(),
		IsWrite:  true,
	})
}

func validateVSearch() ValidationHook {
	return func(args []string) error {
		if len(args) < 2 {
			return fmt.Errorf("expected minimum 2 argument, got %d", len(args))
		}
		return nil
	}
}

func executeVSearch() ExecutionHook {
	return func(args []string, store store.Store) string {
		result, err := store.VSearch(args)
		if err != nil {
			return resp.EncodeError(err.Error())
		}
		return resp.Encode2DStringArrayRESP(result)
	}
}
