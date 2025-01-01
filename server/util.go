package server

import "treds/resp"

func parseCommand(inp string) (string, []string, error) {
	command, args, err := resp.Decode(inp)
	if err != nil {
		return "", nil, err
	}
	return command, args, nil
}

func unique(inps []string) []string {
	// make inps unique
	// use map to make it unique
	uniqueMap := make(map[string]struct{})
	for _, inp := range inps {
		uniqueMap[inp] = struct{}{}
	}
	uniqueInps := make([]string, 0)
	for inp := range uniqueMap {
		uniqueInps = append(uniqueInps, inp)
	}
	return uniqueInps
}
