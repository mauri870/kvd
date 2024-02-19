package kvstore

import "io"

type KV interface {
	// Get returns the value for the given key.
	Get(key []byte) ([]byte, error)
	// Set sets the value for the given key.
	Set(key, value []byte) error
	// Delete deletes the value for the given key.
	Delete(key []byte) error
	// Snapshot returns a snapshot of the kv state.
	Snapshot(io.Writer) error
	// Restore restores the kv to a previous state.
	Restore(io.Reader) error
}
