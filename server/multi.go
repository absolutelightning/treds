package server

import (
	"fmt"

	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const MultiCommandName = "MULTI"

func RegisterMultiCommand(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    MultiCommandName,
		Execute: executeMulti(),
	})
}

func executeMulti() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
		// Only writes need to be forwarded to leader
		// Process the command on the leader
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

		// Check for transaction first, if transaction just enqueue the command
		if _, ok := ts.GetClientTransaction()[c.RemoteAddr().String()]; ok {
			_, errConn := c.Write([]byte(resp.EncodeError("MULTI calls cannot be nested")))
			if errConn != nil {
				ts.RespondErr(c, errConn)
			}
			return gnet.None
		}

		ts.LockClientTransaction()
		defer ts.UnlockClientTransaction()

		ts.GetClientTransaction()[c.RemoteAddr().String()] = make([]string, 0)

		res := "OK"
		_, errConn := c.Write([]byte(resp.EncodeSimpleString(res)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None
	}

}
