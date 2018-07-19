package internal

import (
	"errors"
	"math"
)

/**
The base implementation for all random string pickers.

Returns errors if it ever detects it has entered an inconsistent state as a
result of concurrent access, but does not try to reliably detect misuse.
*/
type Base struct {
	closed bool
	r      random
	t      *Rbtree
	bias   float64
}

func NewBasePicker() *Base {
	return &Base{r: newDefaultRandom(), t: &Rbtree{}, bias: 2}
}

// A Base picker that always returns the leftmost, oldest element
// For testing purposes only
func NewLeftmostOldestBasePicker() *Base {
	return &Base{r: newFakeRandom([]int{0}, []float64{0}), t: &Rbtree{}, bias: 2}
}

func (b *Base) Add(s string) (bool, int, error) {
	if b.closed {
		return false, 0, ErrClosed
	}

	g := b.addGeneration()

	return b.t.insert(s, g), g, nil
}
func (b *Base) AddAll(ss []string) ([]bool, int, error) {
	if b.closed {
		return nil, 0, ErrClosed
	}

	g := b.addGeneration()

	out := make([]bool, len(ss), len(ss))
	for i, s := range ss {
		out[i] = b.t.insert(s, g)
	}

	return out, g, nil
}

// For loading data from a database
func (b *Base) Load(s string, g int) (bool, error) {
	if b.closed {
		return false, ErrClosed
	}

	return b.t.insert(s, g), nil
}

func (b *Base) LoadAll(ss []string, g int) ([]bool, error) {
	if b.closed {
		return nil, ErrClosed
	}

	out := make([]bool, len(ss), len(ss))
	for i, s := range ss {
		out[i] = b.t.insert(s, g)
	}

	return out, nil
}

// Only gets called when loading values from a DB
func (b *Base) LoadDB(ss []string, gs []int) ([]bool, error) {
	if b.closed {
		return nil, ErrClosed
	}
	if len(ss) != len(gs) {
		return nil, errors.New(
			"Mismatch between number of strings generations in call to Load()")
	}

	out := make([]bool, len(ss), len(ss))
	for i, s := range ss {
		out[i] = b.t.insert(s, gs[i])
	}

	return out, nil
}

func (b *Base) Remove(s string) (bool, error) {
	if b.closed {
		return false, ErrClosed
	}

	return b.t.delete(s), nil
}
func (b *Base) RemoveAll(ss []string) ([]bool, error) {
	if b.closed {
		return nil, ErrClosed
	}

	out := make([]bool, len(ss), len(ss))
	for i, s := range ss {
		out[i] = b.t.delete(s)
	}

	return out, nil
}

// Returns the new generation assigned to the string, not the old generation
func (b *Base) Next() (string, int, error) {
	if b.closed {
		return "", 0, ErrClosed
	}
	if b.t.size == 0 {
		return "", 0, ErrEmpty
	}

	g := b.nextGeneration()
	if g == int(^uint(0)>>1) {
		return "", 0, ErrOverflow
	}

	rbn, err := b.findNext()
	if err != nil {
		return "", 0, nil
	}

	rbn.gen = g
	rbn.recalcAncestors()

	return rbn.key, g, nil
}

func (b *Base) NextN(n int) ([]string, int, error) {
	if b.closed {
		return nil, 0, ErrClosed
	}
	if b.t.size == 0 {
		return nil, 0, ErrEmpty
	}
	if n < 0 {
		return nil, 0, ErrNegative
	}
	g := b.nextGeneration()
	if g == int(^uint(0)>>1) {
		return nil, 0, ErrOverflow
	}

	out := make([]string, n, n)
	for i := range out {
		rbn, err := b.findNext()
		if err != nil {
			// Should only happen if the tree is damaged from concurrent access
			return nil, 0, err
		}

		out[i] = rbn.key

		rbn.gen = g
		rbn.recalcAncestors()
	}
	return out, g, nil
}

/**
Force unique values by removing items from the tree after selection.

Fails if n > Size().
*/
func (b *Base) UniqueN(n int) ([]string, int, error) {
	if b.closed {
		return nil, 0, ErrClosed
	}
	if b.t.size == 0 {
		return nil, 0, ErrEmpty
	}
	if n < 0 {
		return nil, 0, ErrNegative
	}
	if b.t.size < n {
		return nil, 0, ErrInsufficientUnique
	}

	g := b.nextGeneration()
	if g == int(^uint(0)>>1) {
		return nil, 0, ErrOverflow
	}

	out := make([]string, n, n)
	for i := range out {
		rbn, err := b.findNext()
		if err != nil {
			// Should only happen if the tree is damaged from concurrent access
			// Don't bother attempting to repair it from the damage we've done here
			return nil, 0, err
		}
		out[i] = rbn.key
		b.t.delete(out[i])
	}
	for _, s := range out {
		b.t.insert(s, g)
	}

	return out, g, nil
}

func (b *Base) findNext() (*rbnode, error) {
	index := b.r.Intn(b.t.size)
	gen := b.randomWeightedGeneration()

	return b.t.findNext(index, gen)
}

func (b *Base) Contains(s string) bool {
	return b.t.findNode(s) != nil
}

func (b *Base) SetBias(bi float64) error {
	if b.closed {
		return ErrClosed
	}
	if math.IsNaN(bi) {
		return ErrNaN
	}
	if bi < 0 {
		return ErrNegative
	}

	b.bias = bi
	return nil
}

func (b *Base) Size() (int, error) {
	if b.closed {
		return 0, ErrClosed
	}

	return b.t.size, nil
}

func (b *Base) Values() ([]string, error) {
	if b.closed {
		return nil, ErrClosed
	}
	return b.t.values(), nil
}

func (b *Base) Close() error {
	b.closed = true
	b.t = nil
	b.r = nil
	return nil
}

func (b *Base) Closed() error {
	if b.closed {
		return ErrClosed
	}
	return nil
}

func (b *Base) MinGen() int {
	if b.t != nil && b.t.root != nil {
		return b.t.root.minGen
	}
	return 0
}

// Newly inserted elements are considered as old as the oldest item in the tree
func (b *Base) addGeneration() int {
	if b.t.root == nil {
		return 0
	}

	return b.t.root.minGen
}

func (b *Base) nextGeneration() int {
	return b.t.root.maxGen + 1
}

// Bias towards the lower end
func (b *Base) randomWeightedGeneration() int {
	if b.t.size == 1 {
		return b.t.root.gen
	}

	span := b.t.root.maxGen - b.t.root.minGen
	// Add one and use Floor() to ensure it can pick every possible generation
	offset := float64(span+1) * math.Pow(b.r.Float64(), b.bias)
	floor := int(math.Floor(offset))
	if floor > span {
		// Should not happen
		floor = span
	}

	// Floor is biased towards 0
	return b.t.root.minGen + floor
}

// TODO -- rework errors to accept more information
/*
func closedError() error {
	pcs := make([]uintptr, 1)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	baseFrame, _ := frames.Next()
	fn := baseFrame.Function
	function := fn[strings.LastIndex(fn, ".")+1:]
	return fmt.Errorf("%s() called on closed Picker", function)
}
func emptyError() error {
	pcs := make([]uintptr, 1)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	baseFrame, _ := frames.Next()
	fn := baseFrame.Function
	function := fn[strings.LastIndex(fn, ".")+1:]
	return fmt.Errorf("%s() called on empty Picker", function)
}
*/
