package internal

import (
	"math"
	"reflect"
	"testing"
)

func TestSingleElement(t *testing.T) {
	b := NewBasePicker()
	_, g, err := b.Add("a")
	if err != nil {
		t.Error(err)
	}
	verifySize(t, b, 1)

	if !b.Contains("a") {
		t.Error("Base unexpectedly doesn't contain a")
	}
	if b.Contains("b") {
		t.Error("Base unexpectedly contains b")
	}

	v, g2, err := b.Next()
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g2, g)
	if v != "a" {
		t.Errorf("Unexpected string returned, got [%s]", v)
	}

	vs, g3, err := b.NextN(1)
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g3, g2)
	if !reflect.DeepEqual(vs, []string{"a"}) {
		t.Errorf("Unexpected strings returned, got [%v]", vs)
	}

	vs, g4, err := b.NextN(2)
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g4, g3)
	if !reflect.DeepEqual(vs, []string{"a", "a"}) {
		t.Errorf("Unexpected strings returned, got [%v]", vs)
	}

	vs, g5, err := b.UniqueN(1)
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g5, g4)
	if !reflect.DeepEqual(vs, []string{"a"}) {
		t.Errorf("Unexpected strings returned, got [%v]", vs)
	}

	v, g6, err := b.Next()
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g6, g5)
	if v != "a" {
		t.Errorf("Unexpected string returned, got [%s]", v)
	}

	_, _, err = b.UniqueN(2)
	verifyError(t, err, ErrInsufficientUnique)

	removed, err := b.Remove("a")
	if err != nil {
		t.Error(err)
	}
	if !removed {
		t.Error("RemoveAll() on tree with one element unexpected returned false")
	}

	verifySize(t, b, 0)
}

func TestAlwaysLeftmostOldest(t *testing.T) {
	b := NewLeftmostOldestBasePicker()

	added, g, err := b.AddAll([]string{"e"})
	if err != nil {
		t.Fatal(err)
	}
	if len(added) != 1 || !added[0] || g != 0 {
		t.Fatalf("Unexpected output from AddAll, got %v %d", added, g)
	}

	loaded, err := b.Load("d", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded {
		t.Fatalf("Unexpected output from Load, got %v", loaded)
	}

	loads, err := b.LoadAll([]string{"c", "d", "e"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loads, []bool{true, false, false}) {
		t.Fatalf("Unexpected output from LoadAll, got %v", loads)
	}

	// The 9s should be ignored
	_, err = b.LoadDB([]string{"a", "b", "c", "d", "e"}, []int{4, 2, 9, 9, 9})
	g = 4
	if err != nil {
		t.Fatal(err)
	}
	verifySize(t, b, 5)

	v, g2, err := b.Next()
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g2, g)
	if v != "e" {
		t.Errorf("Unexpected string returned, got [%s]", v)
	}

	vs, g3, err := b.NextN(1)
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g3, g2)
	if !reflect.DeepEqual(vs, []string{"d"}) {
		t.Errorf("Unexpected strings returned, got [%v]", vs)
	}

	vs, g4, err := b.NextN(2)
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g4, g3)
	if !reflect.DeepEqual(vs, []string{"b", "c"}) {
		t.Errorf("Unexpected strings returned, got [%v]", vs)
	}

	vs, g5, err := b.UniqueN(1)
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g5, g4)
	if !reflect.DeepEqual(vs, []string{"a"}) {
		t.Errorf("Unexpected strings returned, got [%v]", vs)
	}

	vs, g6, err := b.UniqueN(5)
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g6, g5)
	if !reflect.DeepEqual(vs, []string{"e", "d", "b", "c", "a"}) {
		t.Errorf("Unexpected strings returned, got [%v]", vs)
	}

	// All the values share the same generation now
	vs, g7, err := b.NextN(8)
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g7, g6)
	if !reflect.DeepEqual(vs, []string{"a", "b", "c", "d", "e", "a", "a", "a"}) {
		t.Errorf("Unexpected strings returned, got [%v]", vs)
	}

	v, g8, err := b.Next()
	if err != nil {
		t.Error(err)
	}
	verifyNewGeneration(t, g8, g7)
	if v != "a" {
		t.Errorf("Unexpected string returned, got [%s]", v)
	}

	_, _, err = b.UniqueN(6)
	verifyError(t, err, ErrInsufficientUnique)

	removed, err := b.RemoveAll([]string{"a", "b", "c", "d", "e", "f"})
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(removed, []bool{true, true, true, true, true, false}) {
		t.Errorf(
			"RemoveAll() on tree with five elements returned unexpected values %v",
			removed)
	}

	verifySize(t, b, 0)
}
func TestOverflow(t *testing.T) {
	b := NewBasePicker()
	b.LoadDB([]string{"a", "b"}, []int{0, int(^uint(0)>>1) - 1})

	// Overflow detection
	_, _, err := b.Next()
	verifyError(t, err, ErrOverflow)
	_, _, err = b.NextN(1)
	verifyError(t, err, ErrOverflow)
	_, _, err = b.UniqueN(1)
	verifyError(t, err, ErrOverflow)
}

func TestBaseEmpty(t *testing.T) {
	b := NewBasePicker()

	_, _, err := b.Next()
	verifyError(t, err, ErrEmpty)
	_, _, err = b.NextN(1)
	verifyError(t, err, ErrEmpty)
	_, _, err = b.UniqueN(1)
	verifyError(t, err, ErrEmpty)
}

func TestInvalidNumbers(t *testing.T) {
	b := NewBasePicker()

	b.Add("a")

	_, _, err := b.NextN(-1)
	verifyError(t, err, ErrNegative)
	_, _, err = b.UniqueN(-1)
	verifyError(t, err, ErrNegative)

	err = b.SetBias(-1)
	verifyError(t, err, ErrNegative)
	err = b.SetBias(math.Inf(-1))
	verifyError(t, err, ErrNegative)
	err = b.SetBias(math.NaN())
	verifyError(t, err, ErrNaN)
}

func TestBaseClosed(t *testing.T) {
	b := NewBasePicker()
	b.Close()

	_, _, err := b.Add("a")
	verifyError(t, err, ErrClosed)
	_, _, err = b.AddAll([]string{"a"})
	verifyError(t, err, ErrClosed)
	_, err = b.Load("a", 1)
	verifyError(t, err, ErrClosed)
	_, err = b.LoadAll([]string{"a"}, 1)
	verifyError(t, err, ErrClosed)
	_, err = b.LoadDB([]string{"a"}, []int{1})
	verifyError(t, err, ErrClosed)
	_, err = b.Remove("a")
	verifyError(t, err, ErrClosed)
	_, err = b.RemoveAll([]string{"a"})
	verifyError(t, err, ErrClosed)
	_, _, err = b.Next()
	verifyError(t, err, ErrClosed)
	_, _, err = b.NextN(5)
	verifyError(t, err, ErrClosed)
	_, _, err = b.UniqueN(5)
	verifyError(t, err, ErrClosed)
	err = b.SetBias(100)
	verifyError(t, err, ErrClosed)
	_, err = b.GetBias()
	verifyError(t, err, ErrClosed)
	_, err = b.Size()
	verifyError(t, err, ErrClosed)
	_, err = b.Values()
	verifyError(t, err, ErrClosed)
}

func TestRandomWeightedGeneration(t *testing.T) {
	b := Base{
		r: newFakeRandom([]int{}, []float64{0, 1, 0.5}), t: &rbtree{}, bias: 2}

	b.LoadDB([]string{"0", "1"}, []int{11, 111})
	// Test that the bounds hold even in an impossible case
	// (Float64 returns [0, 1), not [0, 1])
	if g := b.randomWeightedGeneration(); g != 11 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 11)
	}
	if g := b.randomWeightedGeneration(); g != 111 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 111)
	}

	// Test that it's properly biased towards the low end
	// 0.5 in a range of 100 (111-11) is 25 from the minimum (11+25=36)
	if g := b.randomWeightedGeneration(); g != 36 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 36)
	}

	b.Remove("0")
	if g := b.randomWeightedGeneration(); g != 111 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 111)
	}

	b = Base{
		r: newFakeRandom([]int{}, []float64{0, 1, 0.5}), t: &rbtree{}, bias: 1}
	b.LoadDB([]string{"0", "1"}, []int{11, 111})

	if g := b.randomWeightedGeneration(); g != 11 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 11)
	}
	if g := b.randomWeightedGeneration(); g != 111 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 111)
	}
	if g := b.randomWeightedGeneration(); g != 61 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 61)
	}

	_ = b.SetBias(0.5)

	if g := b.randomWeightedGeneration(); g != 11 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 11)
	}
	if g := b.randomWeightedGeneration(); g != 111 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 111)
	}
	if g := b.randomWeightedGeneration(); g != 82 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 82)
	}

	_ = b.SetBias(0)

	if g := b.randomWeightedGeneration(); g != 111 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 111)
	}
	if g := b.randomWeightedGeneration(); g != 111 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 111)
	}
	if g := b.randomWeightedGeneration(); g != 111 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 111)
	}

	_ = b.SetBias(math.Inf(1))

	if g := b.randomWeightedGeneration(); g != 11 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 11)
	}
	if g := b.randomWeightedGeneration(); g != 111 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 111)
	}
	if g := b.randomWeightedGeneration(); g != 11 {
		t.Errorf("Unexpected generation produced, got %d expected %d", g, 11)
	}
}

func verifyNewGeneration(t *testing.T, new int, old int) {
	if new != old+1 {
		t.Errorf(
			"New generation [%d] must be old generation [%d] plus one", new, old)
	}
}

func verifySize(t *testing.T, b *Base, size int) {
	if s, err := b.Size(); err != nil || s != size {
		t.Errorf("Unexpected values returned by Size(), got [%d, %v] expected [%d]",
			s, err, size)
	}
}

func verifyError(t *testing.T, err error, expected error) {
	if err == nil {
		t.Errorf("Expected error [%s] not thrown", expected)
	} else if err != expected {
		t.Errorf(
			"Expected error [%s] not thrown, got [%s] instead", expected, err)
	}
}
