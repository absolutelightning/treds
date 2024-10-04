package server

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	"github.com/panjf2000/gnet/v2"
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
	return commandReg.Execute(commandStringParts[1:], t.tredsStore)
}

type snapshot struct {
	storageSnapshot []byte
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.storageSnapshot); err != nil {
		return err
	}
	if err := sink.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot sink: %v", err)
	}
	return nil
}

func (s *snapshot) Release() {}

func (t TredsFsm) Snapshot() (raft.FSMSnapshot, error) {
	defer func(start time.Time) {
		log.Println("snapshot created", "duration", time.Since(start).String())
	}(time.Now())

	storageSnapshot, err := t.tredsStore.Snapshot()
	if err != nil {
		return nil, err
	}

	return &snapshot{
		storageSnapshot: storageSnapshot,
	}, nil
}

func (t TredsFsm) Restore(old io.ReadCloser) error {
	defer old.Close()
	data, err := io.ReadAll(old)
	if err != nil {
		return err
	}
	ts := store.NewTredsStore()
	err = ts.Restore(data)
	if err != nil {
		return err
	}
	t.tredsStore = ts
	return nil
}

func NewTredsFsm(registry commands.CommandRegistry, store store.Store) *TredsFsm {
	return &TredsFsm{cmdRegistry: registry, tredsStore: store}
}
