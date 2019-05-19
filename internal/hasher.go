package internal

import (
	"hash/fnv"
	"time"
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
		salt: []byte(time.Now().String()),
	}
}

type defaultHasher struct {
	// Salt is only used to randomize order each time the tree is built
	salt []byte
}

func (dh *defaultHasher) hash(s string) uint64 {
	h := fnv.New64a()
	h.Write(dh.salt)
	h.Write([]byte(s))
	return h.Sum64()
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
