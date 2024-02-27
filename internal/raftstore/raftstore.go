package raftstore

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/mauri870/kvd/internal/kvstore"
)

var (
	ErrNotALeader = fmt.Errorf("not a leader")
)

type Store struct {
	dir         string
	raftAddress string
	inmem       bool
	kv          kvstore.KV
	raft        *raft.Raft
	timeout     time.Duration
	logWriter   io.Writer
}

func New(kv kvstore.KV, dir, address string, inmem bool, logWriter io.Writer) *Store {
	return &Store{
		dir:         dir,
		raftAddress: address,
		inmem:       inmem,
		kv:          kv,
		logWriter:   logWriter,
		timeout:     10 * time.Second,
	}
}

func (s *Store) Open(enableSingle bool, id string) error {
	// setup raft configuration
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.raftAddress)
	if err != nil {
		return err
	}

	// listen on TCP
	transport, err := raft.NewTCPTransport(s.raftAddress, addr, 3, 5*time.Second, s.logWriter)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	retainSnapshotCount := 2
	snapshots, err := raft.NewFileSnapshotStore(s.dir, retainSnapshotCount, s.logWriter)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %w", err)
	}

	logStore, stableStore, err := s.createRaftStore()
	if err != nil {
		return fmt.Errorf("failed to create raft store: %w", err)
	}

	slog.Info("Creating Raft instance")
	raftNode, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("failed to create new raft instance: %w", err)
	}
	s.raft = raftNode

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}

	return nil
}

// createRaftStore creates the raft log store and stable store
func (s *Store) createRaftStore() (raft.LogStore, raft.StableStore, error) {
	if s.inmem {
		return raft.NewInmemStore(), raft.NewInmemStore(), nil
	}

	// in disk
	store, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(s.dir, "raft.db"),
	})
	return store, store, err
}

// kvCmd is a command to be applied to the key-value store.
// It is serialized and written to the Raft log.
type kvCmd struct {
	Op    string
	Key   []byte
	Value []byte
}

// Set sets the value for the given key. Since all modifications must go
// through Raft, only the leader can process it. This is asynchronous,
// the raft subsystem will call the Apply method of the FSM.
func (s *Store) Set(key, value []byte) error {
	// Only the leader can process writes
	if s.raft.State() != raft.Leader {
		return ErrNotALeader
	}

	// We need a blob of bytes to pass to the Raft log. We could have
	// used any kind of serialization here, such as protobuf or gob. We
	// could also apply compression with gzip or snappy to improve disk
	// space and IO. But for simplicity, we'll just use JSON.
	b, err := json.Marshal(kvCmd{Op: "set", Key: key, Value: value})
	if err != nil {
		return err
	}

	// apply the command to the Raft log
	f := s.raft.Apply(b, s.timeout)
	return f.Error()
}

// Get returns the value for the given key.
func (s *Store) Get(key []byte) ([]byte, error) {
	// This can be called without going through Raft, since it is a read
	// operation.
	return s.kv.Get(key)
}

// Delete deletes the given key. Since all modifications must go
// through Raft, only the leader can process it. This is asynchronous,
// the raft subsystem will call the Apply method of the FSM.
func (s *Store) Delete(key []byte) error {
	// Only the leader can process writes
	if s.raft.State() != raft.Leader {
		return ErrNotALeader
	}

	// We need a blob of bytes to pass to the Raft log. We could have
	// used any kind of serialization here, such as protobuf or gob. We
	// could also apply compression with gzip or snappy to improve disk
	// space and IO. But for simplicity, we'll just use JSON.
	b, err := json.Marshal(kvCmd{Op: "delete", Key: key})
	if err != nil {
		return err
	}

	// apply the command to the Raft log
	f := s.raft.Apply(b, s.timeout)
	return f.Error()
}

// Join joins a node to the raft store, identified by nodeID and network address.
func (s *Store) Join(nodeID, addr string) error {
	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// check if the server is already member of the cluster
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				slog.Warn("node is already member of cluster, ignoring join request", "nodeID", nodeID, "addr", addr)
				return nil
			}

			// remove the existing node since the nodeID and addr don't match
			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %w", nodeID, addr, err)
			}
		}
	}

	// add the new node to the cluster
	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	slog.Info("node joined the cluster successfully", "nodeID", nodeID, "addr", addr)
	return nil
}

// Leave removes a node from the raft store
func (s *Store) Leave(nodeID string) error {
	// Only the leader can remove nodes
	if s.raft.State() != raft.Leader {
		return ErrNotALeader
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get raft configuration: %w", err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) {
			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s : %w", nodeID, err)
			}
			return nil
		}
	}
	return nil
}

// fsm is the finite state machine that the Raft subsystem will use to
// apply log entries to the key-value store.
type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	// Unmarshal the log entry.
	var cmd kvCmd
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command, bad data in log entry: %w", err)
	}

	return f.applyCmd(cmd)
}

func (f *fsm) applyCmd(cmd kvCmd) interface{} {
	switch cmd.Op {
	case "set":
		return f.kv.Set(cmd.Key, cmd.Value)
	case "delete":
		return f.kv.Delete(cmd.Key)
	default:
		return fmt.Errorf("unrecognized command op: %s", cmd.Op)
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{fsm: f}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	return f.kv.Restore(rc)
}

type fsmSnapshot struct {
	fsm *fsm
}

// Persist writes the FSM snapshot to the given sink.
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		err := f.fsm.kv.Snapshot(sink)
		if err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
