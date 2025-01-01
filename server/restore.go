package server

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/panjf2000/gnet/v2"
	"treds/resp"
)

const RestoreCommandName = "RESTORE"

func RegisterRestoreCommand(r ServerCommandRegistry) {
	r.Add(&ServerCommandRegistration{
		Name:    RestoreCommandName,
		Execute: executeRestore(),
	})
}

func executeRestore() ExecutionHook {
	return func(inp string, ts *Server, c gnet.Conn) gnet.Action {
		_, args, err := parseCommand(inp)
		if err != nil {
			ts.RespondErr(c, err)
			return gnet.None
		}

		if len(args) != 1 {
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

		if _, ok := ts.GetClientTransaction()[c.RemoteAddr().String()]; ok {
			ts.RespondErr(c, fmt.Errorf("please run this command outside transaction"))
			return gnet.None
		}

		snapshotPath := args[0]

		metaFile := filepath.Join(snapshotPath, "meta.json")

		// Read the file contents
		metaData, err := os.ReadFile(metaFile)
		if err != nil {
			fmt.Println("Error reading file:", err)
			ts.RespondErr(c, err)
			return gnet.None
		}

		// Unmarshal the JSON into the SnapshotMeta struct
		var metaSnapshot *raft.SnapshotMeta
		err = json.Unmarshal(metaData, &metaSnapshot)
		if err != nil {
			fmt.Println("Error unmarshaling JSON:", err)
			ts.RespondErr(c, err)
			return gnet.None
		}

		file, err := os.Open(filepath.Join(snapshotPath, "state.bin"))
		if err != nil {
			fmt.Println("Error opening file:", err)
			ts.RespondErr(c, err)
			return gnet.None
		}
		// Ensure the file is closed when done
		defer file.Close()

		// Since *os.File implements io.Reader, you can directly use it as an io.Reader
		var reader io.Reader = file

		err = ts.GetRaft().Restore(metaSnapshot, reader, 2*time.Minute)
		if err != nil {
			ts.RespondErr(c, err)
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
