package cache

import (
	"encoding/json"
	"net/textproto"
	"time"

	"github.com/alecthomas/errors"
	"go.etcd.io/bbolt"
)

var metadataBucketName = []byte("metadata")

// metadata stores expiration time and headers for a cache entry.
type metadata struct {
	ExpiresAt time.Time            `json:"expires_at"`
	Headers   textproto.MIMEHeader `json:"headers"`
}

// ttlStorage manages expiration times and headers for cache entries using bbolt.
type ttlStorage struct {
	db *bbolt.DB
}

// newTTLStorage creates a new bbolt-backed TTL storage.
func newTTLStorage(dbPath string) (*ttlStorage, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, errors.Errorf("failed to open bbolt database: %w", err)
	}

	// Create the bucket if it doesn't exist
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(metadataBucketName)
		return errors.WithStack(err)
	}); err != nil {
		return nil, errors.Join(errors.Errorf("failed to create metadata bucket: %w", err), db.Close())
	}

	return &ttlStorage{db: db}, nil
}

func (s *ttlStorage) set(key Key, expiresAt time.Time, headers textproto.MIMEHeader) error {
	md := metadata{
		ExpiresAt: expiresAt,
		Headers:   headers,
	}

	mdBytes, err := json.Marshal(md)
	if err != nil {
		return errors.Errorf("failed to encode metadata: %w", err)
	}

	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucketName)
		return bucket.Put(key[:], mdBytes)
	}))
}

func (s *ttlStorage) get(key Key) (time.Time, textproto.MIMEHeader, error) {
	var md metadata
	err := s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucketName)
		mdBytes := bucket.Get(key[:])
		if mdBytes == nil {
			return errors.New("key not found")
		}
		return errors.WithStack(json.Unmarshal(mdBytes, &md))
	})
	return md.ExpiresAt, md.Headers, errors.WithStack(err)
}

func (s *ttlStorage) delete(key Key) error {
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucketName)
		return bucket.Delete(key[:])
	}))
}

func (s *ttlStorage) deleteAll(keys []Key) error {
	if len(keys) == 0 {
		return nil
	}
	return errors.WithStack(s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucketName)
		for _, key := range keys {
			if err := bucket.Delete(key[:]); err != nil {
				return errors.Errorf("failed to delete metadata: %w", err)
			}
		}
		return nil
	}))
}

func (s *ttlStorage) walk(fn func(key Key, expiresAt time.Time) error) error {
	return errors.WithStack(s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(metadataBucketName)
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			if len(k) != 32 {
				return nil
			}
			var key Key
			copy(key[:], k)
			var md metadata
			if err := json.Unmarshal(v, &md); err != nil {
				return nil //nolint:nilerr
			}
			return fn(key, md.ExpiresAt)
		})
	}))
}

func (s *ttlStorage) close() error {
	if err := s.db.Close(); err != nil {
		return errors.Errorf("failed to close bbolt database: %w", err)
	}
	return nil
}
