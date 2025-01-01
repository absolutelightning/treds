package server

import (
	"fmt"

	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const PubSubChannelCommandName = "PUBSUBCHANNELS"

func RegisterPubSubChannels(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    PubSubChannelCommandName,
		Execute: executePubSubChannelsCommand(),
	})
}

func executePubSubChannelsCommand() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
		_, args, err := parseCommand(inp)
		if err != nil {
			ts.RespondErr(c, err)
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

		prefix := ""

		if len(args) >= 1 {
			prefix = args[0]
		}

		result := make([]string, 0)

		iterator := subscriptionData.Root().Iterator()

		iterator.SeekPrefix([]byte(prefix))

		for {
			key, value, found := iterator.Next()
			if !found {
				break
			}
			subscribers := value.(map[string]struct{})
			if len(subscribers) > 0 {
				result = append(result, string(key))
			}
		}

		_, errConn := c.Write([]byte(resp.EncodeStringArray(result)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None
	}
}
