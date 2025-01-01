package server

import (
	"fmt"
	"strings"

	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const PublishCommandName = "PUBLISH"
const Message = "message"

func RegisterPublishCommand(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    PublishCommandName,
		Execute: executePublishCommand(),
	})
}

func executePublishCommand() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
		_, args, err := parseCommand(inp)
		if err != nil {
			ts.RespondErr(c, err)
			return gnet.None
		}

		if len(args) <= 1 {
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

		message := strings.Join(args[1:], " ")

		subscriptionData := ts.GetChannelSubscriptionData()

		channel := args[0]
		value, found := subscriptionData.Get([]byte(channel))

		countChannelsNotified := 0
		if !found {
			_, errConn := c.Write([]byte(resp.EncodeInteger(countChannelsNotified)))
			if errConn != nil {
				ts.RespondErr(c, errConn)
			}
			return gnet.None
		}

		connections := value.(map[int]struct{})
		for id := range connections {
			arrayMessage := []string{Message, channel, channel, message}
			conn := ts.GetConnectionFromFD(id)
			_, errConn := conn.Write([]byte(resp.EncodeStringArray(arrayMessage)))
			if errConn != nil {
				fmt.Println("Error occurred writing to connection", errConn)
			}
			countChannelsNotified++
		}

		_, errConn := c.Write([]byte(resp.EncodeInteger(countChannelsNotified)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None
	}
}
