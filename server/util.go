package server

import "treds/resp"

func parseCommand(inp string) (string, []string, error) {
	command, args, err := resp.Decode(inp)
	if err != nil {
		return "", nil, err
	}
	return command, args, nil
}
