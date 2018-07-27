package internal

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestInsert(t *testing.T) {
	rb := &rbtree{}

	rb.insert("5", 0)
	rb.insert("4", 1)
	r := rb.insert("6", 2)

	if !r {
		t.Errorf("Insert unexpectedly returned false")
	}

	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 0 b (4 1 r  ) (6 2 r  ))")
}

func TestInsert_leftOnly(t *testing.T) {
	rb := &rbtree{}

	rb.insert("5", 0)
	rb.insert("4", 1)
	rb.insert("3", 2)
	rb.insert("2", 3)
	rb.insert("1", 4)
	r := rb.insert("1", 50) // silently ignored

	if r {
		t.Errorf("Insert for already present value returned true")
	}

	verifyTree(t, rb)
	if rb.size != 5 {
		t.Errorf("Tree has unexpected size %d, expected %d", rb.size, 5)
	}
	verifyTreeStructure(t, rb, "(4 1 b (2 3 b (1 4 r  ) (3 2 r  )) (5 0 b  ))")
}

func TestInsert_rightOnly(t *testing.T) {
	rb := &rbtree{}

	rb.insert("1", 0)
	rb.insert("2", 1)
	rb.insert("3", 2)
	rb.insert("4", 3)
	rb.insert("5", 4)
	rb.insert("5", 50) // silently ignored

	verifyTree(t, rb)
	if rb.size != 5 {
		t.Errorf("Tree has unexpected size %d, expected %d", rb.size, 5)
	}
	verifyTreeStructure(t, rb, "(2 1 b (1 0 b  ) (4 3 b (3 2 r  ) (5 4 r  )))")
}

func TestInsert_leftRight(t *testing.T) {
	rb := &rbtree{}

	rb.insert("5", 0)
	rb.insert("2", 1)
	rb.insert("3", 2)

	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(3 2 b (2 1 r  ) (5 0 r  ))")
}

func TestInsert_rightLeft(t *testing.T) {
	rb := &rbtree{}

	rb.insert("2", 1)
	rb.insert("5", 0)
	rb.insert("3", 2)

	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(3 2 b (2 1 r  ) (5 0 r  ))")
}

func TestInsertShuffled100000(t *testing.T) {
	keys := sequentualStrings(10000)
	rand.Shuffle(10000, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	rb := &rbtree{}
	for i, k := range keys {
		rb.insert(k, i)
	}
	verifyTree(t, rb)
}

func TestDelete_root(t *testing.T) {
	rb := &rbtree{}

	rb.insert("5", 5)
	rb.insert("2", 2)
	rb.insert("7", 7)
	rb.insert("1", 1)
	rb.insert("3", 3)
	rb.insert("6", 6)
	rb.insert("8", 8)

	r := rb.delete("5")
	if !r {
		t.Errorf("Delete for present value returned false")
	}
	verifyTree(t, rb)
	verifyTreeStructure(
		t, rb, "(6 6 b (2 2 b (1 1 r  ) (3 3 r  )) (7 7 b  (8 8 r  )))")

	rb.delete("6")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(7 7 b (2 2 b (1 1 r  ) (3 3 r  )) (8 8 b  ))")

	rb.delete("7")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(2 2 b (1 1 b  ) (8 8 b (3 3 r  ) ))")

	rb.delete("2")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(3 3 b (1 1 b  ) (8 8 b  ))")

	rb.delete("3")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(8 8 b (1 1 r  ) )")

	rb.delete("8")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(1 1 b  )")

	rb.delete("1")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "")

	rb.insert("1", 0)
	rb.insert("2", 0)
	rb.delete("1")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(2 0 b  )")
}

func TestDelete_RedSibling(t *testing.T) {
	rb := &rbtree{}

	rb.insert("1", 0)
	rb.insert("2", 0)
	rb.insert("3", 0)
	rb.insert("4", 0)
	rb.insert("5", 0)
	rb.insert("6", 0)
	verifyTreeStructure(t, rb, "(2 0 b (1 0 b  ) (4 0 r (3 0 b  ) (5 0 b  (6 0 r  ))))")

	rb.delete("1")
	verifyTreeStructure(t, rb, "(4 0 b (2 0 b  (3 0 r  )) (5 0 b  (6 0 r  )))")

	rb = &rbtree{}
	rb.insert("6", 0)
	rb.insert("5", 0)
	rb.insert("4", 0)
	rb.insert("3", 0)
	rb.insert("2", 0)
	rb.insert("1", 0)
	verifyTreeStructure(t, rb, "(5 0 b (3 0 r (2 0 b (1 0 r  ) ) (4 0 b  )) (6 0 b  ))")

	rb.delete("6")
	verifyTreeStructure(t, rb, "(3 0 b (2 0 b (1 0 r  ) ) (5 0 b (4 0 r  ) ))")
}

func TestDelete_SiblingOneInnerRedChild(t *testing.T) {
	rb := &rbtree{}

	rb.insert("1", 0)
	rb.insert("2", 0)
	rb.insert("4", 0)
	rb.insert("3", 0)
	verifyTreeStructure(t, rb, "(2 0 b (1 0 b  ) (4 0 b (3 0 r  ) ))")

	rb.delete("1")
	verifyTreeStructure(t, rb, "(3 0 b (2 0 b  ) (4 0 b  ))")

	rb = &rbtree{}
	rb.insert("4", 0)
	rb.insert("3", 0)
	rb.insert("1", 0)
	rb.insert("2", 0)
	verifyTreeStructure(t, rb, "(3 0 b (1 0 b  (2 0 r  )) (4 0 b  ))")

	rb.delete("4")
	verifyTreeStructure(t, rb, "(2 0 b (1 0 b  ) (3 0 b  ))")
}

func TestDelete_Leaves(t *testing.T) {
	rb := &rbtree{}

	rb.insert("5", 5)
	rb.insert("2", 2)
	rb.insert("7", 7)
	rb.insert("1", 1)
	rb.insert("3", 3)
	rb.insert("6", 6)
	rb.insert("8", 8)

	rb.delete("8")
	verifyTree(t, rb)
	verifyTreeStructure(
		t, rb, "(5 5 b (2 2 b (1 1 r  ) (3 3 r  )) (7 7 b (6 6 r  ) ))")

	rb.delete("1")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b (2 2 b  (3 3 r  )) (7 7 b (6 6 r  ) ))")

	rb.delete("6")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b (2 2 b  (3 3 r  )) (7 7 b  ))")

	rb.delete("3")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b (2 2 b  ) (7 7 b  ))")

	rb.delete("2")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b  (7 7 r  ))")

	rb.delete("7")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b  )")
}

func TestDelete_branch(t *testing.T) {
	rb := &rbtree{}

	rb.insert("5", 5)
	rb.insert("2", 2)
	rb.insert("7", 7)
	rb.insert("1", 1)
	rb.insert("3", 3)
	rb.insert("6", 6)
	rb.insert("8", 8)

	rb.delete("2")
	verifyTree(t, rb)
	verifyTreeStructure(
		t, rb, "(5 5 b (3 3 b (1 1 r  ) ) (7 7 b (6 6 r  ) (8 8 r  )))")

	rb.delete("3")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b (1 1 b  ) (7 7 b (6 6 r  ) (8 8 r  )))")

	rb.delete("7")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b (1 1 b  ) (8 8 b (6 6 r  ) ))")
}

func TestDelete_unbalance(t *testing.T) {
	rb := &rbtree{}

	rb.insert("5", 5)
	rb.insert("2", 2)
	rb.insert("7", 7)
	rb.insert("1", 1)
	rb.insert("3", 3)
	rb.insert("6", 6)
	rb.insert("8", 8)

	rb.delete("2")
	rb.delete("3")
	rb.delete("1")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(7 7 b (5 5 b  (6 6 r  )) (8 8 b  ))")
}

func TestDelete_noop(t *testing.T) {
	rb := &rbtree{}

	r := rb.delete("23423")
	if r {
		t.Errorf("Delete for absent value returned true")
	}
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "")

	rb.insert("5", 5)
	rb.insert("2", 2)
	rb.insert("7", 7)

	rb.delete("8")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b (2 2 r  ) (7 7 r  ))")

	rb.delete("")
	verifyTree(t, rb)
	verifyTreeStructure(t, rb, "(5 5 b (2 2 r  ) (7 7 r  ))")
}

func TestFindNext(t *testing.T) {
	rb := &rbtree{}
	for i, k := range sequentualStrings(11) {
		rb.insert(k, 10-i)
	}

	testLookup(t, rb, 0, 10, "00")
	testLookup(t, rb, 0, 0, "10")
	testLookup(t, rb, 0, 1, "09")
	testLookup(t, rb, 0, 5, "05")
	testLookup(t, rb, 8, 5, "08")
	testLookup(t, rb, 8, 9, "08")
	testLookup(t, rb, 8, 2, "08")
	testLookup(t, rb, 8, 1, "09")
	testLookup(t, rb, 10, 0, "10")
	testLookup(t, rb, 10, 1, "10")
	testLookup(t, rb, 10, 5, "10")
	testLookup(t, rb, 10, 10, "10")
}
func TestFindNext_Reverse(t *testing.T) {
	rb := &rbtree{}
	for i, k := range sequentualStrings(11) {
		if i != 0 {
			rb.insert(k, i)
		} else {
			rb.insert(k, 5)
		}
	}

	testLookup(t, rb, 0, 10, "00")
	testLookup(t, rb, 0, 4, "01")
	testLookup(t, rb, 0, 1, "01")
	testLookup(t, rb, 0, 5, "00")
	testLookup(t, rb, 8, 5, "00")
	testLookup(t, rb, 8, 9, "08")
	testLookup(t, rb, 8, 2, "01")
	testLookup(t, rb, 8, 1, "01")
	testLookup(t, rb, 10, 1, "01")
	testLookup(t, rb, 10, 5, "00")
	testLookup(t, rb, 10, 10, "10")
}

// These methods are only called from Base,
// so any error means the tree is corrupt
func TestFindNext_invalid(t *testing.T) {
	rb := &rbtree{}
	for i, k := range sequentualStrings(10) {
		rb.insert(k, i)
	}

	rb.insert("10", 0)
	_, err := rb.findNext(-1, 0)
	if err != ErrCorrupt {
		t.Errorf("Expected error not thrown for index -1, got %v", err)
	}
	_, err = rb.findNext(11, 0)
	if err != ErrCorrupt {
		t.Errorf("Expected error not thrown for index 11, got %v", err)
	}
	_, err = rb.findNext(5, -1)
	if err != ErrCorrupt {
		t.Errorf("Expected error not thrown for generation -1, got %v", err)
	}
}

func TestValues(t *testing.T) {
	rb := &rbtree{}
	keys := sequentualStrings(10)
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	for i, k := range keys {
		rb.insert(k, i)
	}

	keys = sequentualStrings(10)
	v := rb.values()
	if !reflect.DeepEqual(v, keys) {
		t.Errorf("Unexpected output from values(), got %v expected %v", v, keys)
	}
}

func testLookup(t *testing.T, rb *rbtree, i, g int, e string) {
	n, err := rb.findNext(i, g)
	if err != nil {
		t.Error(err)
		return
	}
	if n == nil {
		t.Errorf("No next node found for (%d, %d)", i, g)
		return
	}
	if n.key != e {
		t.Errorf(
			"Wrong node found for (%d, %d), got %s expected %s", i, g, n.key, e)
	}
}

/** Benchmarks */
func sequentualStrings(n int) []string {
	var keys []string
	l := len(strconv.Itoa(n))
	for i := 0; i < n; i++ {
		k := strconv.Itoa(i)
		keys = append(keys, strings.Repeat("0", l-len(k))+k)
	}

	return keys[:]
}

func benchmarkInserts(b *testing.B, keys []string) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb := &rbtree{}

		for i, k := range keys {
			rb.insert(k, i)
		}
	}
}

// Sequential inserts
func BenchmarkInsert1(b *testing.B) {
	benchmarkInserts(b, sequentualStrings(1))
}
func BenchmarkInsert10(b *testing.B) {
	benchmarkInserts(b, sequentualStrings(10))
}
func BenchmarkInsert100(b *testing.B) {
	benchmarkInserts(b, sequentualStrings(100))
}
func BenchmarkInsert10000(b *testing.B) {
	benchmarkInserts(b, sequentualStrings(10000))
}
func BenchmarkInsert1000000(b *testing.B) {
	benchmarkInserts(b, sequentualStrings(1000000))
}

func BenchmarkInsertShuffled10000(b *testing.B) {
	keys := sequentualStrings(10000)
	rand.Shuffle(10000, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	benchmarkInserts(b, keys)
}

func BenchmarkInsertDelete_FullTree(b *testing.B) {
	n := 1000000
	rb := &rbtree{}

	keys := sequentualStrings(n)
	for i, k := range keys {
		rb.insert(k, i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		x := rand.Intn(n)
		rb.delete(keys[x])
		rb.insert(keys[x], x)
	}
}
func BenchmarkFindNextIn_5(b *testing.B) {
	benchmarkFindNext(b, 5)
}
func BenchmarkFindNextIn_1000(b *testing.B) {
	benchmarkFindNext(b, 1000)
}
func BenchmarkFindNextIn_100000(b *testing.B) {
	benchmarkFindNext(b, 100000)
}

func benchmarkFindNext(b *testing.B, n int) {
	rb := &rbtree{}

	keys := sequentualStrings(n)
	rand.Shuffle(n, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for i, k := range keys {
		rb.insert(k, i)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = rb.findNext(rand.Intn(n), rand.Intn(n))
	}
}

// Verifies all the pointers and properties make some kind of sense
func verifyTree(t *testing.T, rb *rbtree) {
	if rb.root == nil {
		if rb.size != 0 {
			t.Error("Tree with nil root has non-zero size")
		}
		return
	}

	if rb.size != rb.root.children+1 {
		t.Errorf("Tree size %d doesn't match expected size %d",
			rb.size, rb.root.children+1)
	}

	if rb.root.parent != nil {
		t.Error("Tree root has non-nil parent")
	}

	if rb.root.red {
		t.Error("Tree root is red")
	}

	verifySubTree(t, rb.root)
}

// Returns the maximum number of black nodes encountered
func verifySubTree(t *testing.T, n *rbnode) int {
	if n == nil {
		return 0
	}
	bl := verifySubTree(t, n.left)
	br := verifySubTree(t, n.right)

	if br != bl {
		t.Errorf(
			"Node %s violates the equal numbers of black nodes constraint", n.key)
	}

	ming := n.gen
	maxg := n.gen
	c := 0

	if n.left != nil {
		if n.red && n.left.red {
			t.Errorf("Red node %s has red child %s", n.key, n.left.key)
		}

		if n.left.parent != n {
			badParent := "nil"
			if n.left.parent != nil {
				badParent = n.left.parent.key
			}
			t.Errorf("Node %s has incorrect parent %s, expected %s",
				n.left.key, badParent, n.key)
		}
		c += n.left.children + 1
		if n.left.minGen < ming {
			ming = n.left.minGen
		}
		if n.left.maxGen > maxg {
			maxg = n.left.maxGen
		}
	}
	if n.right != nil {
		if n.red && n.right.red {
			t.Errorf("Red node %s has red child %s", n.key, n.right.key)
		}

		if n.right.parent != n {
			badParent := "nil"
			if n.right.parent != nil {
				badParent = n.right.parent.key
			}
			t.Errorf("Node %s has incorrect parent %s, expected %s",
				n.right.key, badParent, n.key)
		}
		c += n.right.children + 1
		if n.right.minGen < ming {
			ming = n.right.minGen
		}
		if n.right.maxGen > maxg {
			maxg = n.right.maxGen
		}
	}

	if c != n.children {
		t.Errorf("Node %s has incorrect children count %d, expected %d",
			n.key, n.children, c)
	}
	if ming != n.minGen {
		t.Errorf("Node %s has incorrect minGen %d, expected %d",
			n.key, n.minGen, ming)
	}
	if maxg != n.maxGen {
		t.Errorf("Node %s has incorrect maxGen %d, expected %d",
			n.key, n.maxGen, maxg)
	}

	if !n.red {
		return br + 1
	}
	return br
}

func verifyTreeStructure(t *testing.T, rb *rbtree, expected string) {
	actual := printTreeStructure(rb.root)
	if expected != actual {
		t.Errorf(
			"Tree has unexpected structure\n%s\nexpected:\n%s", actual, expected)
	}
}

// Prints the tree structure, making it easy to verify the tree
func printTreeStructure(n *rbnode) string {
	if n == nil {
		return ""
	}

	clr := "b"
	if n.red {
		clr = "r"
	}

	return fmt.Sprintf("(%s %d %s %s %s)",
		n.key, n.gen, clr, printTreeStructure(n.left), printTreeStructure(n.right))
}
