package server

import (
	"fmt"
	"io"
	"log"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

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
	tredsStore  atomic.Pointer[store.Store]
	conn        gnet.Conn
}

func (t TredsFsm) Apply(log *raft.Log) interface{} {
	inp := string(log.Data)
	commandStringParts := parseCommand(inp)
	commandReg, err := t.cmdRegistry.Retrieve(strings.ToUpper(commandStringParts[0]))
	if err != nil {
		return err
	}
	currentStore := t.tredsStore.Load()
	if currentStore != nil {
		return commandReg.Execute(commandStringParts[1:], *currentStore)
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

func (t TredsFsm) Snapshot() (raft.FSMSnapshot, error) {
	defer func(start time.Time) {
		log.Println("snapshot created", "duration", time.Since(start).String())
	}(time.Now())

	currentStore := t.tredsStore.Load()
	if currentStore != nil {
		storageSnapshot, err := (*currentStore).Snapshot()
		if err != nil {
			return nil, err
		}
		return &snapshot{
			storageSnapshot: storageSnapshot,
		}, nil
	}
	return nil, fmt.Errorf(NilStore)
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
	t.tredsStore.Store((*store.Store)(unsafe.Pointer(ts)))
	return nil
}

func NewTredsFsm(registry commands.CommandRegistry, store store.Store) *TredsFsm {
	fsm := &TredsFsm{cmdRegistry: registry}
	fsm.tredsStore.Store(&store)
	return fsm
}
