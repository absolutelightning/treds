package server

import (
	"fmt"

	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const DiscardCommandName = "DISCARD"

func RegisterDiscardCommand(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    DiscardCommandName,
		Execute: executeDiscard(),
	})
}

func executeDiscard() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
		// Execute this command at leader
		forwarded, rspFwd, err := ts.ForwardRequest(([]byte)(inp))
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

		delete(ts.GetClientTransaction(), c.RemoteAddr().String())

		res := "OK"
		_, errConn := c.Write([]byte(resp.EncodeSimpleString(res)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None

	}
}
