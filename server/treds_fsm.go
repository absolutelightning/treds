package server

import (
	"github.com/hashicorp/raft"
	"github.com/panjf2000/gnet/v2"
	"io"
	"strings"
	"treds/commands"
	"treds/store"
)

type TredsFsm struct {
	cmdRegistry commands.CommandRegistry
	tredsStore  store.Store
	conn        gnet.Conn
}

func (t TredsFsm) Apply(log *raft.Log) interface{} {
	inp := string(log.Data)
	commandStringParts := parseCommand(inp)
	commandReg, err := t.cmdRegistry.Retrieve(strings.ToUpper(commandStringParts[0]))
	if err != nil {
		return err
	}
	res, err := commandReg.Execute(commandStringParts[1:], t.tredsStore)
	if err != nil {
		return err
	}
	return res
}

func (t TredsFsm) Snapshot() (raft.FSMSnapshot, error) {
	//TODO: implement snapshot creation
	//This need to read the full im mem data, serialize it and write it to the snapshot
	panic("implement me")
}

func (t TredsFsm) Restore(_ io.ReadCloser) error {
	//TODO: implement snapshot creation
	//This need to read from the snapshot, parse the commands,
	//use the registry to retrieve the right command and execute it with the data
	panic("implement me")
}

func NewTredsFsm(registry commands.CommandRegistry, store store.Store) *TredsFsm {
	return &TredsFsm{cmdRegistry: registry, tredsStore: store}
}
