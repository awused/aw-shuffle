package internal

import (
	"time"

	"github.com/segmentio/fasthash/fnv1a"
	"github.com/twmb/murmur3"
)

/*
A simple stable hasher to randomize iteration order.

Used to avoid the case where you add "100" and "101" to an existing picker
and "100" is vastly more likely to be picked than "101".
*/

type hasher interface {
	hash(string) uint64
}

func newDefaultHasher() hasher {
	return &defaultHasher{
		salted: fnv1a.AddBytes64(fnv1a.Init64, []byte(time.Now().String())),
	}
}

type defaultHasher struct {
	// Salt is only used to randomize order each time the tree is built
	salted uint64
}

func (dh *defaultHasher) hash(s string) uint64 {
	// fnv1a has a significant advantage on tiny strings, but quickly falls behind.
	if len(s) < 10 {
		return fnv1a.AddString64(dh.salted, s)
	}
	return murmur3.SeedStringSum64(dh.salted, s)
}

type fakeHasher struct {
	values map[string]uint64
}

func newFakeHasher() *fakeHasher {
	return &fakeHasher{
		values: make(map[string]uint64),
	}
}

func (fh *fakeHasher) hash(s string) uint64 {
	return fh.values[s]
}
