package server

import (
	"fmt"
	"strings"

	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const SubscribeCommandName = "SUBSCRIBE"

func RegisterSubscribeCommand(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    SubscribeCommandName,
		Execute: executeSubscribeCommandName(),
	})
}

func executeSubscribeCommandName() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
		_, args, err := parseCommand(inp)
		if err != nil {
			ts.RespondErr(c, err)
			return gnet.None
		}

		if len(args) == 0 {
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

		// make args unique
		args = unique(args)

		subscriptionData := ts.GetChannelSubscriptionData()
		// all channels matching unique
		allChannels := make(map[string]struct{})
		for _, channel := range args {
			allChannels[channel] = struct{}{}
		}

		ts.LockChannelSubs()
		defer ts.UnlockChannelSubs()
		for channel := range allChannels {
			prevData, ok := subscriptionData.Get([]byte(channel))
			if !ok {
				prevData = make(map[string]struct{})
			}
			newData := prevData.(map[string]struct{})
			newData[c.RemoteAddr().String()] = struct{}{}
			subscriptionData, _, _ = subscriptionData.Insert([]byte(channel), newData)
		}

		ts.SetChannelSubscriptionData(subscriptionData)
		if _, ok := ts.GetConnectionSubscription()[c.RemoteAddr().String()]; !ok {
			ts.GetConnectionSubscription()[c.RemoteAddr().String()] = make(map[string]struct{})
		}

		response := make([]interface{}, 0)
		for indx, channel := range args {
			response = append(response, strings.ToLower(SubscribeCommandName))
			response = append(response, channel)
			ts.GetConnectionSubscription()[c.RemoteAddr().String()][channel] = struct{}{}
			response = append(response, indx+1)
		}
		_, errConn := c.Write([]byte(resp.EncodeArray(response)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None
	}
}
