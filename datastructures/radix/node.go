package radix

import (
	"bytes"
	"sort"
	"sync/atomic"
	"unsafe"
)

// WalkFn is used when walking the tree. Takes a
// key and value, returning if iteration should
// be terminated.
type WalkFn func(k []byte, v interface{}) bool

// LeafNode is used to represent a value
type LeafNode struct {
	key      []byte
	val      interface{}
	nextLeaf unsafe.Pointer
}

func (n *LeafNode) Key() []byte {
	return n.key
}

func (n *LeafNode) Value() interface{} {
	return n.val
}

func (n *LeafNode) SetNextLeaf(l *LeafNode) {
	atomic.StorePointer(&n.nextLeaf, unsafe.Pointer(l))
}

func (n *LeafNode) GetNextLeaf() *LeafNode {
	return (*LeafNode)(atomic.LoadPointer(&n.nextLeaf))
}

// edge is used to represent an edge node
type edge struct {
	label byte
	node  *Node
}

// Node is an immutable node in the radix tree
type Node struct {

	// leaf is used to store possible leaf
	leaf    *LeafNode
	minLeaf *LeafNode
	maxLeaf *LeafNode

	// prefix is the common prefix we ignore
	prefix []byte

	// Edges should be stored in-order for iteration.
	// We avoid a fully materialized slice to save memory,
	// since in most cases we expect to be sparse
	edges edges
}

func (n *Node) isLeaf() bool {
	return n.leaf != nil
}

func (n *Node) updateMinMaxLeaves() {
	n.minLeaf = nil
	n.maxLeaf = nil
	if n.leaf != nil {
		n.minLeaf = n.leaf
	} else if len(n.edges) > 0 {
		n.minLeaf = n.edges[0].node.minLeaf
	}
	if len(n.edges) > 0 {
		n.maxLeaf = n.edges[len(n.edges)-1].node.maxLeaf
	}
	if n.maxLeaf == nil && n.leaf != nil {
		n.maxLeaf = n.leaf
	}
}

func (n *Node) computeLinks() {
	n.updateMinMaxLeaves()
	if len(n.edges) > 0 {
		if n.minLeaf != n.edges[0].node.minLeaf {
			n.minLeaf.SetNextLeaf(n.edges[0].node.minLeaf)
		}
	}
	for itr := 0; itr < len(n.edges); itr++ {
		maxLFirst, _ := n.edges[itr].node.MaximumLeaf()
		var minLSecond *LeafNode
		if itr+1 < len(n.edges) {
			minLSecond, _ = n.edges[itr+1].node.MinimumLeaf()
		}
		if maxLFirst != nil && minLSecond != nil {
			maxLFirst.SetNextLeaf(minLSecond)
		}
	}
}

func (n *Node) addEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	n.edges = append(n.edges, e)
	if idx != num {
		copy(n.edges[idx+1:], n.edges[idx:num])
		n.edges[idx] = e
	}
}

// Minimum is used to return the minimum value in the tree
func (n *Node) MinimumLeaf() (*LeafNode, bool) {
	if n.minLeaf != nil {
		return n.minLeaf, true
	}
	return nil, false
}

// Maximum is used to return the maximum value in the tree
func (n *Node) MaximumLeaf() (*LeafNode, bool) {
	if n.maxLeaf != nil {
		return n.maxLeaf, true
	}
	return nil, false
}

func (n *Node) replaceEdge(e edge) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= e.label
	})
	if idx < num && n.edges[idx].label == e.label {
		n.edges[idx].node = e.node
		return
	}
	panic("replacing missing edge")
}

func (n *Node) getEdge(label byte) (int, *Node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node) getLowerBoundEdge(label byte) (int, *Node) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	// we want lower bound behavior so return even if it's not an exact match
	if idx < num {
		return idx, n.edges[idx].node
	}
	return -1, nil
}

func (n *Node) delEdge(label byte) {
	num := len(n.edges)
	idx := sort.Search(num, func(i int) bool {
		return n.edges[i].label >= label
	})
	if idx < num && n.edges[idx].label == label {
		copy(n.edges[idx:], n.edges[idx+1:])
		n.edges[len(n.edges)-1] = edge{}
		n.edges = n.edges[:len(n.edges)-1]
	}
}

func (n *Node) Search(k []byte) (interface{}, bool) {
	search := k
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			if n.isLeaf() {
				return n.leaf.val, true
			}
			break
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Update to the finest granularity as the search makes progress

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	return nil, false
}

func (n *Node) Get(k []byte) (interface{}, bool) {
	val, ok := n.Search(k)
	return val, ok
}

// LongestPrefix is like Get, but instead of an
// exact match, it will return the longest prefix match.
func (n *Node) LongestPrefix(k []byte) ([]byte, interface{}, bool) {
	var last *LeafNode
	search := k
	for {
		// Look for a leaf node
		if n.isLeaf() {
			last = n.leaf
		}

		// Check for key exhaution
		if len(search) == 0 {
			break
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
	if last != nil {
		return last.key, last.val, true
	}
	return nil, nil, false
}

// Minimum is used to return the minimum value in the tree
func (n *Node) Minimum() ([]byte, interface{}, bool) {
	for {
		if n.isLeaf() {
			return n.leaf.key, n.leaf.val, true
		}
		if len(n.edges) > 0 {
			n = n.edges[0].node
		} else {
			break
		}
	}
	return nil, nil, false
}

// Maximum is used to return the maximum value in the tree
func (n *Node) Maximum() ([]byte, interface{}, bool) {
	for {
		if num := len(n.edges); num > 0 {
			n = n.edges[num-1].node
			continue
		}
		if n.isLeaf() {
			return n.leaf.key, n.leaf.val, true
		} else {
			break
		}
	}
	return nil, nil, false
}

// Iterator is used to return an iterator at
// the given node to walk the tree
func (n *Node) Iterator() *Iterator {
	return &Iterator{node: n}
}

// Walk is used to walk the tree
func (n *Node) Walk(fn WalkFn) {
	recursiveWalk(n, fn)
}

// WalkBackwards is used to walk the tree in reverse order
func (n *Node) WalkBackwards(fn WalkFn) {
	reverseRecursiveWalk(n, fn)
}

// WalkPrefix is used to walk the tree under a prefix
func (n *Node) WalkPrefix(prefix []byte, fn WalkFn) {
	search := prefix
	for {
		// Check for key exhaution
		if len(search) == 0 {
			recursiveWalk(n, fn)
			return
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			break
		}

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]

		} else if bytes.HasPrefix(n.prefix, search) {
			// Child may be under our search prefix
			recursiveWalk(n, fn)
			return
		} else {
			break
		}
	}
}

// WalkPath is used to walk the tree, but only visiting nodes
// from the root down to a given leaf. Where WalkPrefix walks
// all the entries *under* the given prefix, this walks the
// entries *above* the given prefix.
func (n *Node) WalkPath(path []byte, fn WalkFn) {
	search := path
	for {
		// Visit the leaf values if any
		if n.leaf != nil && fn(n.leaf.key, n.leaf.val) {
			return
		}

		// Check for key exhaution
		if len(search) == 0 {
			return
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			return
		}

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]
		} else {
			break
		}
	}
}

// recursiveWalk is used to do a pre-order walk of a node
// recursively. Returns true if the walk should be aborted
func recursiveWalk(n *Node, fn WalkFn) bool {
	// Visit the leaf values if any
	if n.leaf != nil && fn(n.leaf.key, n.leaf.val) {
		return true
	}

	// Recurse on the children
	for _, e := range n.edges {
		if recursiveWalk(e.node, fn) {
			return true
		}
	}
	return false
}

// reverseRecursiveWalk is used to do a reverse pre-order
// walk of a node recursively. Returns true if the walk
// should be aborted
func reverseRecursiveWalk(n *Node, fn WalkFn) bool {
	// Visit the leaf values if any
	if n.leaf != nil && fn(n.leaf.key, n.leaf.val) {
		return true
	}

	// Recurse on the children in reverse order
	for i := len(n.edges) - 1; i >= 0; i-- {
		e := n.edges[i]
		if reverseRecursiveWalk(e.node, fn) {
			return true
		}
	}
	return false
}
