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
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Picker extends strpick.Picker with the additional methods related to
// persisting the state the the Picker to disk. None of these methods are
// required. Close() must be called to safely close the database.
type Picker interface {
	strpick.Picker

	// SoftRemove removes a string from the Picker without removing it from the
	// underlying database. A subsequent Add or LoadDB call will restore its
	// current generation.
	SoftRemove(string) error
	// SoftRemoveAll removes strings from the Picker without removing them from
	// the underlying database. A subsequent Add or LoadDB call will restore
	// their current generations.
	SoftRemoveAll([]string) error

	// LoadDB loads all existing data from the database.
	// Calling this is not necessary, but can be substantially more efficient
	// than calling Add or AddAll.
	LoadDB() error
	// CleanDB deletes any strings not currently present (returned by Values())
	// in this Picker from the database. This includes any strings that have been
	// removed using SoftRemove().
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

// NewPicker creates a new persist.Picker backed by a database in the provided
// directory dir, which will be created if it does not exist. Acquires a
// lock on the database, preventing multiple processes from accessing it at
// once.
// Writes are all performed synchronously.
// Close() must be called to safely close the database.
func NewPicker(dir string) (Picker, error) {
	// Bloom filters use O(1) extra space per SSTable (O(log(n) overall) to
	// enhance read performance. This is beneficial when adding new strings to a
	// very large, and has minimal impact on smaller trees.
	o := &opt.Options{
		Filter: filter.NewBloomFilter(10),
	}

	db, err := leveldb.OpenFile(dir, o)
	if errors.IsCorrupted(err) {
		db, err = leveldb.RecoverFile(dir, o)
	}
	if err != nil {
		return nil, err
	}

	p := &persist{b: internal.NewBasePicker(), m: &sync.Mutex{}, db: db}

	return p, p.loadProperties()
}

func (t *persist) Add(s string) error {
	defer t.m.Unlock()
	t.m.Lock()

	err := t.b.Closed()
	if err != nil {
		return err
	}

	if t.b.Contains(s) {
		return nil
	}

	data, err := t.db.Get(stringToByteKey(s), nil)
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
		return t.loadAndPutGen(s, t.minGen)
	}

	return err
}
func (t *persist) AddAll(ss []string) error {
	defer t.m.Unlock()
	t.m.Lock()

	err := t.b.Closed()
	if err != nil {
		return err
	}

	var dbMiss []string // Could preallocate, likely not worth it

	for _, s := range ss {
		if t.b.Contains(s) {
			continue
		}

		// TODO -- DB lookups here can be parallelized
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
		err = t.loadAndPutGen(s, t.minGen)
		if err != nil {
			return err
		}
	}

	if len(dbMiss) > 0 {
		loaded, err := t.b.LoadAll(ss, t.minGen)
		if err != nil {
			return err
		}
		return t.batchPutGen(dbMiss, t.minGen, loaded)
	}

	return nil
}

// Remove/RemoveAll will not remove a string from the DB unless it is present
// in the live tree
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

	err = t.batchPutGen(ss, g, nil)
	if err != nil {
		return ss, err
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

	err = t.batchPutGen(ss, g, nil)
	if err != nil {
		return ss, err
	}
	return ss, t.checkMinGen()
}
func (t *persist) TryUniqueN(n int) ([]string, error) {
	defer t.m.Unlock()
	t.m.Lock()

	ss, g, err := t.b.UniqueN(n)
	if err == strpick.ErrInsufficientUnique {
		ss, g, err = t.b.NextN(n)
	}
	if err != nil {
		return ss, err
	}

	err = t.batchPutGen(ss, g, nil)
	if err != nil {
		return ss, err
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

// SoftRemove removes a string from the picker without deleting it from the
// database.
func (t *persist) SoftRemove(s string) error {
	defer t.m.Unlock()
	t.m.Lock()
	_, err := t.b.Remove(s)
	if err != nil {
		return err
	}
	return t.checkMinGen()
}

// SoftRemoveAll removes multiple strings from the picker without deleting them
// from the database.
func (t *persist) SoftRemoveAll(ss []string) error {
	defer t.m.Unlock()
	t.m.Lock()
	_, err := t.b.RemoveAll(ss)
	if err != nil {
		return err
	}
	return t.checkMinGen()
}

// LoadDB loads all strings and generations from the database.
func (t *persist) LoadDB() error {
	defer t.m.Unlock()
	t.m.Lock()

	err := t.b.Closed()
	if err != nil {
		return err
	}

	iter := t.db.NewIterator(
		&util.Range{Start: []byte("s:"), Limit: []byte("t")}, nil)

	for iter.Next() {
		gen64, n := binary.Varint(iter.Value())
		var g int
		if n > 0 {
			g = int(gen64)
		} else {
			// Failed to read it, there's no possible recovery
			// Set g to whatever minGen is now
			g = t.minGen
		}

		t.b.Load(byteKeyToString(iter.Key()), g)
	}

	err = iter.Error()
	if err != nil {
		return err
	}

	return t.checkMinGen()
}

// CleanDB removes any strings not currently present in the picker from the
// database. This includes strings removed by SoftRemove().
func (t *persist) CleanDB() error {
	defer t.m.Unlock()
	t.m.Lock()

	err := t.b.Closed()
	if err != nil {
		return err
	}

	valid, err := t.b.Values()
	if err != nil {
		return err
	}
	i := 0

	iter := t.db.NewIterator(
		&util.Range{Start: []byte("s:"), Limit: []byte("t")}, nil)
	batch := new(leveldb.Batch)

	for iter.Next() {
		s := byteKeyToString(iter.Key())

		for i < len(valid) && s > valid[i] {
			i++
		}
		if i == len(valid) || valid[i] != s {
			batch.Delete(iter.Key())
		}
	}

	if batch.Len() > 0 {
		return t.db.Write(batch, nil)
	}
	return nil
}

// Put generations for all modified keys
// Since we're storing the same gen many times, we can save on allocations
// mask may be nil
func (t *persist) batchPutGen(ss []string, g int, mask []bool) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(g))
	buf = buf[:n]

	batch := new(leveldb.Batch)
	for i, s := range ss {
		if mask == nil || mask[i] {
			batch.Put(stringToByteKey(s), buf)
		}
	}

	if batch.Len() > 0 {
		return t.db.Write(batch, nil)
	}
	return nil
}

// Does _not_ call checkMinGen
func (t *persist) loadAndPutGen(s string, g int) error {
	loaded, err := t.b.Load(s, g)

	if err == nil && loaded {
		err = t.dbPutInt(stringToByteKey(s), g)
	}

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

// checkMinGen checks to see if the minimum generation of the live tree has
// changed from what is stored in the database, and updates it if necessary.
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
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(g))
	return t.db.Put(key, buf[:n], nil)
}
