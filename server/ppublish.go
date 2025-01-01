package server

import (
	"fmt"
	"strings"

	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const PPublishCommandName = "PPUBLISH"
const PMessage = "pmessage"

func RegisterPPublishCommand(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    PPublishCommandName,
		Execute: executePPublishCommand(),
	})
}

func executePPublishCommand() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
		_, args, err := parseCommand(inp)
		if err != nil {
			ts.RespondErr(c, err)
			return gnet.None
		}

		if len(args) <= 0 {
			ts.RespondErr(c, fmt.Errorf("invalid number of arguments"))
			return gnet.None
		}

		// Process this command on leader
		forwarded, rspFwd, err := ts.ForwardRequest([]byte(inp))
		if err != nil {
			ts.RespondErr(c, err)
			return gnet.None
		}

		// If request is forwarded we just send back the answer from the leader to the client
		// and stop processing
		if forwarded {
			_, errConn := c.Write([]byte(rspFwd))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			return gnet.None
		}

		subscriptionData := ts.GetChannelSubscriptionData()

		// make args unique
		channelPrefix := args[0]
		message := strings.Join(args[1:], " ")

		countChannelsNotified := 0

		// all channels matching all args prefix
		iterator := subscriptionData.Root().Iterator()
		iterator.SeekPrefix([]byte(channelPrefix))
		for {
			key, value, found := iterator.Next()
			if !found {
				break
			}
			connections := value.(map[string]struct{})
			for id := range connections {
				arrayMessage := []string{PMessage, channelPrefix, string(key), message}
				conn := ts.GetConnectionFromAddress(id)
				_, errConn := conn.Write([]byte(resp.EncodeStringArray(arrayMessage)))
				if errConn != nil {
					fmt.Println("Error occurred writing to connection", errConn)
				}
				countChannelsNotified++
			}
		}

		_, errConn := c.Write([]byte(resp.EncodeInteger(countChannelsNotified)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None
	}
}
