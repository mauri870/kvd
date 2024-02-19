package kvstore

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

var (
	boltBucket = []byte("kv")

	ErrKeyNotFound    = errors.New("key not found")
	ErrBucketNotFound = errors.New("bucket not found")
)

type BoltKV struct {
	db   *bbolt.DB
	path string
}

func New(path string) (*BoltKV, error) {
	db, err := initializeDB(path)
	if err != nil {
		return nil, err
	}
	return &BoltKV{db: db, path: path}, nil
}

func initializeDB(path string) (*bbolt.DB, error) {
	db, err := bbolt.Open(path, 0o600, bbolt.DefaultOptions)
	if err != nil {
		return nil, err
	}

	// create the kv bucket if it doesn't exist
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (kv *BoltKV) Close() error {
	return kv.db.Close()
}

func (kv *BoltKV) Get(key []byte) ([]byte, error) {
	var value []byte
	err := kv.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		if bucket == nil {
			return ErrBucketNotFound
		}

		v := bucket.Get(key)
		if v == nil {
			return ErrKeyNotFound
		}
		// copy the value to avoid returning a reference to the data.
		value = append([]byte(nil), v...)
		return nil
	})

	return value, err
}

func (kv *BoltKV) Set(key, value []byte) error {
	err := kv.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		if bucket == nil {
			return ErrBucketNotFound
		}
		return bucket.Put(key, value)
	})
	return err
}

func (kv *BoltKV) Delete(key []byte) error {
	err := kv.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		if bucket == nil {
			return ErrBucketNotFound
		}

		v := bucket.Get(key)
		if v == nil {
			return ErrKeyNotFound
		}
		return bucket.Delete(key)
	})
	return err
}

func (kv *BoltKV) Snapshot(w io.Writer) error {
	// write the db snapshot to the writer
	return kv.db.View(func(tx *bbolt.Tx) error {
		_, err := tx.WriteTo(w)
		return err
	})
}

func (kv *BoltKV) Restore(r io.Reader) error {
	// close the db before restoring
	// no lock is needed since Restore is called in isolation
	err := kv.db.Close()
	if err != nil {
		return err
	}

	// create a new temp file to restore into
	f, err := os.CreateTemp(filepath.Dir(kv.path), "*.snapshot")
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	// copy the snapshot to the temp file
	_, err = io.Copy(f, r)
	if err != nil {
		return err
	}

	// move the temp file to the correct path
	err = os.Rename(f.Name(), kv.path)
	if err != nil {
		return err
	}

	// reopen the db
	db, err := initializeDB(kv.path)
	if err != nil {
		return err
	}
	kv.db = db

	return nil
}
