package commands

import (
	"math"
	"strconv"

	"treds/store"
)

const ZREVRANGESCOREKEYS = "ZREVRANGESCOREKEYS"

func RegisterZRevRangeScoreCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZREVRANGESCOREKEYS,
		Validate: validateZRangeScore(),
		Execute:  executeZRevRangeScoreKeys(),
	})
}

func executeZRevRangeScoreKeys() ExecutionHook {
	return func(args []string, store store.Store) string {
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
				return err.Error()
			}
			withScore = includeScore
		}
		v, err := store.ZRevRangeByScoreKeys(args[0], args[1], args[2], startIndex, count, withScore)
		if err != nil {
			return err.Error()
		}
		return v
	}
}
