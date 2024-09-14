package commands

import (
	"fmt"
	"math"
	"strconv"
	"treds/store"
)

const ZRANGESCOREKEYS = "ZRANGESCOREKEYS"

func RegisterZRangeScoreCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZRANGESCOREKEYS,
		Validate: validateZRangeScore(),
		Execute:  executeZRangeScoreKeys(),
	})
}

func validateZRangeScore() ValidationHook {
	return func(args []string) error {
		if len(args) < 3 {
			return fmt.Errorf("expected minimum 3 argument, got %d", len(args))
		}
		if len(args) > 6 {
			return fmt.Errorf("expected maximum 6 argument, got %d", len(args))
		}
		return nil
	}
}

func executeZRangeScoreKeys() ExecutionHook {
	return func(args []string, store store.Store) (string, error) {
		startIndex := strconv.Itoa(0)
		if len(args) > 4 {
			startIndex = args[3]
		}
		count := strconv.Itoa(math.MaxInt64)
		if len(args) > 5 {
			count = args[4]
		}
		withScore := true
		if len(args) > 5 {
			includeScore, err := strconv.ParseBool(args[5])
			if err != nil {
				return "", err
			}
			withScore = includeScore
		}
		v, err := store.ZRangeByScoreKeys(args[0], args[1], args[2], startIndex, count, withScore)
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
