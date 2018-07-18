package persistent

import (
	"encoding/binary"
	"math"
	"strings"
	"sync"

	strpick "github.com/awused/go-strpick"
	"github.com/awused/go-strpick/internal"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
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
	// minGen only tracks the minimum generation of the live tree
	// Older "inactive" values in the DB don't count
	minGen int
}

func NewPicker(dir string) (*persist, error) {
	db, err := leveldb.OpenFile(dir, nil)
	if errors.IsCorrupted(err) {
		db, err = leveldb.RecoverFile(dir, nil)
	}
	if err != nil {
		return nil, err
	}

	p := &persist{b: internal.NewBasePicker(), m: &sync.Mutex{}, db: db}

	return p, p.loadProperties()
}

func (t *persist) Add(s string) error {
	bk := stringToByteKey(s)

	defer t.m.Unlock()
	t.m.Lock()

	if err := t.b.Closed(); err != nil {
		return err
	}

	if t.b.Contains(s) {
		return nil
	}

	data, err := t.db.Get(bk, nil)
	if err == nil {
		// Unless a very long-lived DB is moved from a 64 to 32 bit environment
		// converting from int64 to int won't involve truncation
		gen64, n := binary.Varint(data)
		if n > 0 {
			_, err = t.b.Load(s, int(gen64))
			if err != nil {
				return err
			}
			return t.checkMinGen()
		}
	}

	// If binary.Varint failed err will be nil
	if err == nil || err == leveldb.ErrNotFound {
		return t.load(s, t.minGen)
	}

	return err
}
func (t *persist) AddAll(ss []string) error {
	err := t.b.Closed()
	if err != nil {
		return err
	}

	defer t.m.Unlock()
	t.m.Lock()

	var dbMiss []string // Could preallocate, likely not worth it

	for _, s := range ss {
		if t.b.Contains(s) {
			continue
		}

		data, err := t.db.Get(stringToByteKey(s), nil)
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

	// Loading from the DB could have changed the minimum generation
	// Check this before inserting new elements
	err = t.checkMinGen()
	if err != nil {
		return err
	}

	for _, s := range dbMiss {
		err = t.load(s, t.minGen)
		if err != nil {
			return err
		}
	}

	return nil
}

// buf could be provided as a parameter for increased efficiency in AddAll
// Does _not_ check minGen
func (t *persist) load(s string, g int) error {
	loaded, err := t.b.Load(s, g)

	if err == nil && loaded {
		err = t.dbPutInt(stringToByteKey(s), g)
	}

	return err
}

func (t *persist) Remove(s string) error {
	defer t.m.Unlock()
	t.m.Lock()
	removed, err := t.b.Remove(s)
	if err != nil {
		return err
	}

	if removed {
		err = t.db.Delete(stringToByteKey(s), nil)
		if err != nil {
			return err
		}
		return t.checkMinGen()
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
			err = t.db.Delete(stringToByteKey(ss[i]), nil)
			if err != nil {
				return err
			}
		}
	}
	return t.checkMinGen()
}

func (t *persist) Next() (string, error) {
	defer t.m.Unlock()
	t.m.Lock()

	s, g, err := t.b.Next()
	if err != nil {
		return "", err
	}

	err = t.dbPutInt(stringToByteKey(s), g)
	if err != nil {
		return "", err
	}

	return s, t.checkMinGen()
}
func (t *persist) NextN(n int) ([]string, error) {
	defer t.m.Unlock()
	t.m.Lock()

	ss, g, err := t.b.NextN(n)
	if err != nil {
		return ss, err
	}

	for _, s := range ss {
		err = t.dbPutInt(stringToByteKey(s), g)
		if err != nil {
			return ss, err
		}
	}
	return ss, t.checkMinGen()
}
func (t *persist) UniqueN(n int) ([]string, error) {
	defer t.m.Unlock()
	t.m.Lock()

	ss, g, err := t.b.UniqueN(n)
	if err != nil {
		return ss, err
	}

	for _, s := range ss {
		err = t.dbPutInt(stringToByteKey(s), g)
		if err != nil {
			return ss, err
		}
	}
	return ss, t.checkMinGen()
}

func (t *persist) SetBias(bi float64) error {
	defer t.m.Unlock()
	t.m.Lock()

	if err := t.b.SetBias(bi); err != nil {
		return err
	}

	return t.saveBias(bi)
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

var minGenProp = []byte("p:mingen")
var biasProp = []byte("p:bias")

func (t *persist) loadProperties() error {
	data, err := t.db.Get(minGenProp, nil)
	if err == nil {
		gen64, n := binary.Varint(data)
		if n > 0 {
			t.minGen = int(gen64)
		}
	} else if err != leveldb.ErrNotFound {
		return err
	}

	data, err = t.db.Get(biasProp, nil)
	if err == nil {
		bits := binary.LittleEndian.Uint64(data)
		bias := math.Float64frombits(bits)

		err = t.b.SetBias(bias)

		if err != nil {
			return err
		}
	} else if err != leveldb.ErrNotFound {
		return err
	}

	return nil
}

// It's necessary to call this a lot, because it's possible for minGen to
// change on Add/AddAll if an old value was loaded from the DB
func (t *persist) checkMinGen() error {
	if t.b.MinGen() != t.minGen {
		t.minGen = t.b.MinGen()
		return t.dbPutInt(minGenProp, t.minGen)
	}

	return nil
}

func (t *persist) saveBias(bi float64) error {
	bits := math.Float64bits(bi)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, bits)
	return t.db.Put(biasProp, buf, nil)
}

const keyPrefix = "s:"

func stringToByteKey(s string) []byte {
	return []byte(keyPrefix + s)
}

func byteKeyToString(b []byte) string {
	return strings.Trim(string(b), keyPrefix)
}

func (t *persist) dbPutInt(key []byte, g int) error {
	// Could attach this buffer to the persist struct and reuse it
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(g))
	return t.db.Put(key, buf[:n], nil)
}
