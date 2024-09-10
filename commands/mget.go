package commands

import (
	"bytes"
	"fmt"
	"golang.org/x/sync/errgroup"
	"sync"
	"treds/store"
)

const MGetCommand = "MGET"

func RegisterMGetCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     MGetCommand,
		Validate: validateMGet(),
		Execute:  executeMGet(),
	})
}

func validateMGet() ValidationHook {
	return func(args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("expected atlest 1 argument, got %d", len(args))
		}

		return nil
	}
}

func executeMGet() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		results := make([]string, len(args))

		// Create an error group
		var g errgroup.Group

		// Use a mutex to protect writing to the results slice
		var mu sync.Mutex

		// Loop through each key to retrieve
		for i, arg := range args {
			// Capture the current index and key to avoid closure issues
			index := i
			key := arg

			// Start a new goroutine for each key
			g.Go(func() error {
				// Get the result from the store
				res, err := store.Get(key)
				if err != nil {
					return err // Return error to errgroup
				}

				// Use the mutex to protect access to the results slice
				mu.Lock()
				results[index] = res
				mu.Unlock()

				return nil
			})
		}

		// Wait for all goroutines to finish and check for any errors
		if err := g.Wait(); err != nil {
			return "", err
		}

		// Concatenate results to maintain order
		var response bytes.Buffer
		for _, res := range results {
			response.WriteString(fmt.Sprintf("%v\n", res))
		}

		return response.String(), nil
	}
}
