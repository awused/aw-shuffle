package persistent

import (
	"encoding/binary"
	"sync"

	strpick "github.com/awused/go-strpick"
	"github.com/awused/go-strpick/internal"
	"github.com/syndtr/goleveldb/leveldb"
)

type Picker interface {
	strpick.Picker

	// Loads all existing keys from the database.
	// Calling this is not necessary in the case where the caller can supply
	// every valid string, values will be loaded as needed in Add and AddAll.
	LoadDB() error
	// Removes any keys not _currently_ present in thise Picker from the database
	// Also runs a compaction, should only be called when necessary.
	CleanDB() error
}

/**
A picker that persists its changes to disk using leveldb.

This is the simplest possible implementation of persistence.
Synchronous writes are not used, limiting crash protection, but it's not fully
asynchronous. There is a performance penalty while waiting for the OS write
cache layer to return.

AddAll() or LoadDB() are recommended over individual Add() calls.

Safe for concurrent use from multiple goroutines.
*/
type persist struct {
	b  *internal.Base
	m  *sync.Mutex
	db *leveldb.DB
}

func NewPicker(dir string) (*persist, error) {
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return nil, err
	}

	return &persist{b: internal.NewBasePicker(), m: &sync.Mutex{}, db: db}, nil
}

func (t *persist) Add(s string) error {
	bk := []byte(s)

	defer t.m.Unlock()
	t.m.Lock()

	data, err := t.db.Get(bk, nil)
	if err == nil {
		// Unless a very long-lived DB is moved from a 64 to 32 bit environment
		// converting from int64 to int won't involve truncation
		gen64, n := binary.Varint(data)
		if n > 0 {
			_, err = t.b.Load(s, int(gen64))
			return err
		}
	}

	// If binary.Varint failed err will be nil
	if err == nil || err == leveldb.ErrNotFound {
		add, gen, err := t.b.Add(s)
		if err != nil {
			return err
		}
		if add {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutVarint(buf, int64(gen))
			return t.db.Put(bk, buf[:n], nil)
		}
		return nil
	}

	return err
}
func (t *persist) AddAll(ss []string) error {
	defer t.m.Unlock()
	t.m.Lock()

	var dbMiss []string // Could preallocate, likely not worth it

	for _, s := range ss {
		bk := []byte(s)

		data, err := t.db.Get(bk, nil)
		if err != nil && err != leveldb.ErrNotFound {
			return err
		}

		if err == nil {
			// Unless a very long-lived DB is moved from a 64 to 32 bit environment
			// converting from int64 to int won't involve truncation
			gen64, n := binary.Varint(data)
			if n > 0 {
				_, err = t.b.Load(s, int(gen64))
				if err != nil {
					return err
				}
				continue
			}
		}

		dbMiss = append(dbMiss, s)
	}

	added, gen, err := t.b.AddAll(dbMiss)
	if err != nil {
		return err
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(gen))
	buf = buf[:n]

	for i, a := range added {
		if !a {
			continue
		}
		bk := []byte(dbMiss[i])

		err = t.db.Put(bk, buf, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *persist) Remove(s string) error {
	defer t.m.Unlock()
	t.m.Lock()
	removed, err := t.b.Remove(s)
	if err != nil {
		return err
	}

	if removed {
		return t.db.Delete([]byte(s), nil)
	}
	return nil
}
func (t *persist) RemoveAll(ss []string) error {
	defer t.m.Unlock()
	t.m.Lock()
	removed, err := t.b.RemoveAll(ss)
	if err != nil {
		return err
	}

	for i, r := range removed {
		if r {
			err = t.db.Delete([]byte(ss[i]), nil)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *persist) Next() (string, error) {
	defer t.m.Unlock()
	t.m.Lock()

	s, g, err := t.b.Next()
	if err != nil {
		return "", err
	}

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(g))
	err = t.db.Put([]byte(s), buf[:n], nil)

	return s, err
}
func (t *persist) NextN(n int) ([]string, error) {
	defer t.m.Unlock()
	t.m.Lock()

	ss, g, err := t.b.NextN(n)
	if err != nil {
		return ss, err
	}

	buf := make([]byte, binary.MaxVarintLen64)
	bc := binary.PutVarint(buf, int64(g))
	buf = buf[:bc]

	for _, s := range ss {
		err = t.db.Put([]byte(s), buf, nil)
		if err != nil {
			return ss, err
		}
	}
	return ss, err
}
func (t *persist) UniqueN(n int) ([]string, error) {
	defer t.m.Unlock()
	t.m.Lock()

	ss, g, err := t.b.UniqueN(n)
	if err != nil {
		return ss, err
	}

	buf := make([]byte, binary.MaxVarintLen64)
	bc := binary.PutVarint(buf, int64(g))
	buf = buf[:bc]

	for _, s := range ss {
		err = t.db.Put([]byte(s), buf, nil)
		if err != nil {
			return ss, err
		}
	}
	return ss, err
}

func (t *persist) Size() (int, error) {
	t.m.Lock()
	sz, err := t.b.Size()
	t.m.Unlock()
	return sz, err
}
func (t *persist) Values() ([]string, error) {
	t.m.Lock()
	ss, err := t.b.Values()
	t.m.Unlock()
	return ss, err
}

func (t *persist) Close() error {
	defer t.m.Unlock()
	t.m.Lock()

	// Closing a leveldb instance multiple times is not an error
	err := t.db.Close()
	if err != nil {
		return err
	}

	err = t.b.Close()
	return err
}
