package server

import (
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/panjf2000/gnet/v2"
	"treds/commands"
	"treds/store"
)

const (
	NilStore = "error Nil Store"
)

type TredsFsm struct {
	cmdRegistry commands.CommandRegistry
	tredsStore  store.Store
	conn        gnet.Conn
	storeLock   sync.Mutex
}

func (t *TredsFsm) Apply(log *raft.Log) interface{} {
	inp := string(log.Data)
	command, args, err := parseCommand(inp)
	if err != nil {
		return err
	}
	commandReg, err := t.cmdRegistry.Retrieve(strings.ToUpper(command))
	if err != nil {
		return err
	}
	if commandReg.IsWrite {
		t.storeLock.Lock()
		defer t.storeLock.Unlock()
	}
	currentStore := t.tredsStore
	if currentStore != nil {
		return commandReg.Execute(args, currentStore)
	}
	return NilStore
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

func (t *TredsFsm) Snapshot() (raft.FSMSnapshot, error) {
	defer func(start time.Time) {
		log.Println("snapshot created", "duration", time.Since(start).String())
	}(time.Now())
	fmt.Println("generating snapshot")

	storageSnapshot, err := t.tredsStore.Snapshot()
	if err != nil {
		return nil, err
	}
	return &snapshot{
		storageSnapshot: storageSnapshot,
	}, nil
}

func (t *TredsFsm) Restore(old io.ReadCloser) error {
	fmt.Println("restoring snapshot")
	defer old.Close()
	data, err := io.ReadAll(old)
	if err != nil {
		return err
	}
	ts := store.NewTredsStore()
	err = ts.Restore(data)
	t.tredsStore = ts
	if err != nil {
		return err
	}
	return nil
}

func NewTredsFsm(registry commands.CommandRegistry, store store.Store) *TredsFsm {
	return &TredsFsm{cmdRegistry: registry, tredsStore: store}
}
