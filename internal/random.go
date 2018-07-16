package internal

// Not thread-safe, but all Pickers besides Unsafe provide their own locking

import (
	"math/rand"
	"time"
)

type random interface {
	// Returns a float64 in [0,1)
	Float64() float64

	// Returns an int in [0, n)
	Intn(n int) int
}

func newDefaultRandom() random {
	src := rand.NewSource(time.Now().UnixNano())
	return rand.New(src)
}

// Fake random for tests that simply loops through provided values
// Doesn't validate output of Intn
type fakeRandom struct {
	ii, fi int
	ints   []int
	floats []float64
}

func newFakeRandom(ints []int, floats []float64) *fakeRandom {
	return &fakeRandom{ints: ints, floats: floats}
}

func (r *fakeRandom) Float64() float64 {
	out := r.floats[r.fi]
	r.fi++
	if r.fi >= len(r.floats) {
		r.fi = 0
	}
	return out
}

func (r *fakeRandom) Intn(n int) int {
	out := r.ints[r.ii]
	r.ii++
	if r.ii >= len(r.ints) {
		r.ii = 0
	}
	return out
}
