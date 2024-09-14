package commands

import (
	"math"
	"strconv"
	"treds/store"
)

const ZREVRANGESCOREKVS = "ZREVRANGESCOREKVS"

func RegisterZRevRangeScoreKVSCommand(r CommandRegistry) {
	r.Add(&CommandRegistration{
		Name:     ZREVRANGESCOREKVS,
		Validate: validateZRangeScore(),
		Execute:  executeZRevRangeScoreKVS(),
	})
}

func executeZRevRangeScoreKVS() ExecutionHook {
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
		v, err := store.ZRevRangeByScoreKVS(args[0], args[1], args[2], startIndex, count, withScore)
		if err != nil {
			return "", err
		}
		return v, nil
	}
}
