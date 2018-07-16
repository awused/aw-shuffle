package internal

import (
	"fmt"
)

type rbnode struct {
	key                           string
	red                           bool
	gen, children, minGen, maxGen int
	left, right, parent           *rbnode
}

type Rbtree struct {
	root *rbnode
	size int
}

func (t *Rbtree) insert(k string, g int) bool {
	nd := rbnode{key: k, gen: g, minGen: g, maxGen: g, red: true}

	if t.root == nil {
		t.root = &nd
		nd.red = false
		t.size++
		return true
	}

	// Look for where the new node should be inserted
	c := t.root
	var p *rbnode
	for c != nil {
		if c.key == nd.key {
			return false
		}

		p = c
		if nd.key < c.key {
			c = c.left
		} else {
			c = c.right
		}
	}

	t.size++
	nd.parent = p
	if nd.key < p.key {
		p.left = &nd
	} else {
		p.right = &nd
	}

	// Fix generations and children counters for all ancestors
	for p != nil {
		p.children++
		if g > p.maxGen {
			p.maxGen = g
		} else if g < p.minGen {
			p.minGen = g
		}

		p = p.parent
	}

	// Now restore rb tree properties
	t.fixAfterInsert(&nd)
	return true
}

func (t *Rbtree) delete(k string) bool {
	if t.root == nil {
		return false
	}

	n := t.root
	for true {
		if n == nil {
			// Key doesn't exist in tree
			return false
		}

		if n.key == k {
			break
		} else if k < n.key {
			n = n.left
		} else {
			n = n.right
		}
	}

	t.size--

	if n.right != nil && n.left != nil {
		// Replace n with its successor so it only has one child
		s := n.right
		for s.left != nil {
			s = s.left
		}
		// Only key and gen need to be copied,
		// the rest will be recalculated in the next step
		n.key, s.key = s.key, n.key
		n.gen, s.gen = s.gen, n.gen

		n = s
	}

	p := n.parent
	// Deleting the root, if this happens there's nothing to recalculate
	if p == nil {
		if n.left != nil {
			t.root = n.left
			n.left.parent = nil
			n.left.red = false
		} else if n.right != nil {
			t.root = n.right
			n.right.parent = nil
			n.right.red = false
		} else {
			t.root = nil
		}
		return true
	}

	c := n.left
	if c == nil {
		c = n.right
	}
	// Red n is trivial to remove
	if n.red || (c != nil && c.red) {
		if n.parent.left == n {
			p.left = c
		} else {
			p.right = c
		}

		if c != nil {
			c.red = false
			c.parent = p
		}
	} else {
		t.fixBeforeDelete(n)
		// n now has no children
		if n.parent.left == n {
			n.parent.left = nil
		} else {
			n.parent.right = nil
		}
	}

	n.parent.recalcAncestors()
	return true
}

// Finds the next item with a generation <= g after index
// Wraps around
func (t *Rbtree) findNext(index int, g int) (*rbnode, error) {
	if index < 0 || t.size <= index {
		return nil, ErrCorrupt
		//return nil, fmt.Errorf(
		//"Invalid index [%d] specified for tree with size [%d]", index, t.size)
	}
	if g < t.root.minGen {
		return nil, ErrCorrupt
		//return nil, fmt.Errorf(
		//"Invalid generation [%d] specified for tree with minGen [%d]",
		//g, t.root.minGen)
	}

	n := t.root.findAbove(index, g)
	if n == nil && index != 0 {
		n = t.root.findAbove(0, g)
	}
	if n == nil {
		return nil, ErrCorrupt
	}
	return n, nil
}

// See the notes at the bottom for why recursion is used
func (n *rbnode) findAbove(i int, g int) *rbnode {
	if n.minGen > g {
		return nil
	}

	leftc := 0
	var ret *rbnode

	if n.left != nil {
		leftc = n.left.children + 1

		if i < leftc {
			ret = n.left.findAbove(i, g)
			if ret != nil {
				return ret
			}
		}
	}

	if i < leftc+1 && n.gen <= g {
		return n
	}

	if n.right != nil {
		return n.right.findAbove(i-(leftc+1), g)
	}

	// This will only be executed if the tree is in a broken state
	return nil
}

func (t *Rbtree) fixAfterInsert(c *rbnode) {
	p := c.parent
	for p != nil {
		// Parent is black, we're done
		if !p.red {
			return
		}

		g := p.parent
		ps := g.otherChild(p)
		// parent-sibling is red, recolour and continue up the tree
		if ps != nil && ps.red {
			p.red = false
			ps.red = false
			g.red = true
			c = g
			p = c.parent
			continue
		}

		if g.left == p {
			if p.right == c {
				t.rotateLeft(p)
				p = c
			}
			t.rotateRight(g)
		} else {
			if p.left == c {
				t.rotateRight(p)
				p = c
			}
			t.rotateLeft(g)
		}
		p.red = false
		g.red = true
		return
	}
	// We've replaced the root, and it cannot be red
	c.red = false
}

// This is only called when the node to be deleted is a non-root black node, and therefore has a sibling
func (t *Rbtree) fixBeforeDelete(n *rbnode) {
	for n.parent != nil {
		s := n.parent.otherChild(n) // s can't be nil
		// If the sibling is red, we make it black and rotate so it is where the parent used to be
		if s.red {
			n.parent.red = true
			s.red = false
			if n.parent.left == n {
				t.rotateLeft(n.parent)
			} else {
				t.rotateRight(n.parent)
			}
		}

		s = n.parent.otherChild(n)
		if !n.parent.red && !s.red && (s.left == nil || !s.left.red) && (s.right == nil || !s.right.red) {
			// All three nodes were black and S has no red children
			// Mark S as red so the subtree rooted at n.parent meets the black-path requirement
			// Continue up the tree so that the entire tree is updated to have one less black node in each leaf path
			s.red = true
			n = n.parent
			continue
		}

		if n.parent.red && !s.red && (s.left == nil || !s.left.red) && (s.right == nil || !s.right.red) {
			// Parent is red, so now sibling's subtree has one more black node per path than this subtree
			// Quickly fixed by making S red if S has no red children
			s.red = true
			n.parent.red = false
			return
		}

		if !s.red {
			// All three nodes are black but S has one right child on the inside
			// We can make S red and rotate so the inner child is the new S, followed by a rotation one level up in the opposite direction
			if n == n.parent.left && (s.right == nil || !s.right.red) && (s.left != nil && s.left.red) {
				s.red = true
				s.left.red = false
				t.rotateRight(s)
			} else if n == n.parent.right && (s.left == nil || !s.left.red) && (s.right != nil && s.right.red) {
				s.red = true
				s.right.red = false
				t.rotateLeft(s)
			}
		}

		s = n.parent.otherChild(n)
		// S is either red itself or has two red children
		// n.parent may or may not be red

		// Rotate so S is in n.parent's spot with n.parent's colour and ensure its two children are both black

		s.red = n.parent.red
		n.parent.red = false
		if n.parent.left == n {
			if s != nil && s.right != nil {
				s.right.red = false
			}

			t.rotateLeft(n.parent)
		} else {
			if s != nil && s.left != nil {
				s.left.red = false
			}

			t.rotateRight(n.parent)
		}
		return
	}
}

func (n *rbnode) otherChild(c *rbnode) *rbnode {
	if n.left == c {
		return n.right
	}
	return n.left
}

func (n *rbnode) recalcNode() {
	n.children = 0
	n.maxGen = n.gen
	n.minGen = n.gen

	if n.left != nil {
		n.children += 1 + n.left.children
		if n.left.minGen < n.minGen {
			n.minGen = n.left.minGen
		}
		if n.left.maxGen > n.maxGen {
			n.maxGen = n.left.maxGen
		}
	}

	if n.right != nil {
		n.children += 1 + n.right.children
		if n.right.minGen < n.minGen {
			n.minGen = n.right.minGen
		}
		if n.right.maxGen > n.maxGen {
			n.maxGen = n.right.maxGen
		}
	}
}

func (n *rbnode) recalcAncestors() {
	for n != nil {
		n.recalcNode()
		n = n.parent
	}
}

func (t *Rbtree) rotateRight(p *rbnode) {
	// Left child becomes the new parent
	l := p.left
	p.left = l.right
	if l.right != nil {
		l.right.parent = p
	}
	l.right = p
	l.parent = p.parent
	p.parent = l
	if l.parent != nil {
		if l.parent.right == p {
			l.parent.right = l
		} else {
			l.parent.left = l
		}
	} else {
		t.root = l
	}

	p.recalcNode()
	l.recalcNode()
}

func (t *Rbtree) rotateLeft(p *rbnode) {
	// Right child becomes the new parent
	r := p.right
	p.right = r.left
	if r.left != nil {
		r.left.parent = p
	}
	r.left = p
	r.parent = p.parent
	p.parent = r
	if r.parent != nil {
		if r.parent.right == p {
			r.parent.right = r
		} else {
			r.parent.left = r
		}
	} else {
		t.root = r
	}

	p.recalcNode()
	r.recalcNode()
}

func (t *Rbtree) values() []string {
	output := make([]string, 0, t.size)

	out := &output

	if t.root != nil {
		t.root.values(&out)
	}

	return *out
}

func (n *rbnode) values(out **[]string) {
	if n.left != nil {
		n.left.values(out)
	}

	t := append(**out, n.key)
	(*out) = &t

	if n.right != nil {
		n.right.values(out)
	}
}

// Mostly for debugging
func (t *Rbtree) Pprint() string {
	if t.root == nil {
		return ""
	}
	return t.root.pprint("")
}

func (n *rbnode) pprint(prefix string) string {
	left := ""
	if n.left != nil {
		left = n.left.pprint(prefix + "  ")
	}
	right := ""
	if n.right != nil {
		right = n.right.pprint(prefix + "  ")
	}
	return fmt.Sprintf("%s%s%s: %d, red:%t\n%s", left, prefix, n.key, n.gen, n.red, right)
}

/**
For all but the very largest trees the recursive version of this is faster,
and it's not signficantly slower at higher depths. Recursive depth is limited
to 2*lg(N)

Plus optimizing the iterative version requires keeping the stack as part
of the tree, to avoid constantly reallocating it, which makes for a messier
implementation.

Recursive:
BenchmarkFindNextIn_5-8                 100000000              109 ns/op
BenchmarkFindNextIn_10-8                100000000              115 ns/op
BenchmarkFindNextIn_100-8               100000000              153 ns/op
BenchmarkFindNextIn_1000-8              100000000              205 ns/op
BenchmarkFindNextIn_10000-8             50000000               343 ns/op
BenchmarkFindNextIn_100000-8            30000000               576 ns/op
BenchmarkFindNextIn_1000000-8           10000000              1370 ns/op
BenchmarkFindNextIn_10000000-8           5000000              2598 ns/op

Iterative:
BenchmarkFindNextIn_5-8                 100000000              119 ns/op
BenchmarkFindNextIn_10-8                100000000              128 ns/op
BenchmarkFindNextIn_100-8               100000000              190 ns/op
BenchmarkFindNextIn_1000-8              50000000               258 ns/op
BenchmarkFindNextIn_10000-8             50000000               410 ns/op
BenchmarkFindNextIn_100000-8            20000000               608 ns/op
BenchmarkFindNextIn_1000000-8           10000000              1386 ns/op
BenchmarkFindNextIn_10000000-8           5000000              2476 ns/op

type stackframe struct {
	// The index we're looking for within the subtree rooted at n
	// Can be negative, which means any node in the subtree is valid
	i int
	// The last possible index in the subtree we're looking for
	// For optimization purposes on the second call
	n *rbnode
	// Whether it's the first or second time visiting a node
	second bool
}

func (t *Rbtree) findAbove(index int, g int) *rbnode {
	if t.root.minGen > g {
		return nil
	}

	t.stack = t.stack[:0]
	t.stack = append(t.stack, stackframe{index, t.root, false})

	for len(t.stack) > 0 {
		f := t.stack[len(t.stack)-1]
		t.stack = t.stack[:len(t.stack)-1]

		if f.n.minGen > g {
			continue
		}

		if f.second {
			return f.n
		}

		leftc := 0

		if f.n.left != nil {
			leftc = f.n.left.children + 1
		}

		if (leftc == 0 || f.i == leftc) && f.n.gen <= g {
			// Short circuit if we know the left tree isn't going to be explored
			return f.n
		}

		if f.n.right != nil {
			t.stack = append(t.stack, stackframe{f.i - (leftc + 1), f.n.right, false})
		}

		if f.i < leftc+1 && f.n.gen <= g {
			t.stack = append(t.stack, stackframe{f.i, f.n, true})
		}

		if leftc != 0 && f.i < leftc {
			t.stack = append(t.stack, stackframe{f.i, f.n.left, false})
		}
	}

	return nil
}

*/
