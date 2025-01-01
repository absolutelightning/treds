package server

import (
	"fmt"

	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const SnapshotCommandName = "SNAPSHOT"

func RegisterSnapshotCommand(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    SnapshotCommandName,
		Execute: executeSnapshot(),
	})
}

func executeSnapshot() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
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

		if _, ok := ts.GetClientTransaction()[c.RemoteAddr().String()]; ok {
			ts.RespondErr(c, fmt.Errorf("please run this command outside transaction"))
			return gnet.None
		}

		future := ts.GetRaft().Snapshot()
		if future.Error() != nil {
			ts.RespondErr(c, future.Error())
			return gnet.None
		}
		res := "OK"
		_, errConn := c.Write([]byte(resp.EncodeSimpleString(res)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None
	}
}
