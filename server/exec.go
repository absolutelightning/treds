package server

import (
	"fmt"
	"strings"

	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const ExecCommandName = "EXEC"

func RegisterExecCommand(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    ExecCommandName,
		Execute: executeExec(),
	})
}

func executeExec() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
		//Process this command on leader
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

		ts.LockClientTransaction()
		defer ts.UnlockClientTransaction()

		clientTransaction, ok := ts.GetClientTransaction()[c.RemoteAddr().String()]
		if !ok {
			ts.RespondErr(c, fmt.Errorf("no transaction started"))
			return gnet.None
		}

		replies := make([]string, 0, len(clientTransaction))
		for _, transactionCommand := range clientTransaction {
			storedCommand, storedArgs, errSubCommand := parseCommand(transactionCommand)
			if errSubCommand != nil {
				replies = append(replies, errSubCommand.Error())
				continue
			}

			commandReg, errCommand := ts.GetCommandRegistry().Retrieve(strings.ToUpper(storedCommand))
			if errCommand != nil {
				replies = append(replies, errCommand.Error())
				continue
			}
			// Validation need to be done before raft Apply so an error is returned before persisting
			if errCommand = commandReg.Validate(storedArgs); errCommand != nil {
				replies = append(replies, errCommand.Error())
				continue
			}

			future := ts.GetRaft().Apply([]byte(transactionCommand), ts.GetRaftApplyTimeout())

			if err := future.Error(); err != nil {
				ts.RespondErr(c, err)
				return gnet.None
			}
			rsp := future.Response()

			switch rsp.(type) {
			case error:
				errResp := rsp.(error)
				replies = append(replies, errResp.Error())
			default:
				replies = append(replies, rsp.(string))
			}
		}
		delete(ts.GetClientTransaction(), c.RemoteAddr().String())

		_, errConn := c.Write([]byte(resp.EncodeStringArrayRESP(replies)))
		if errConn != nil {
			ts.RespondErr(c, errConn)
		}
		return gnet.None
	}
}
