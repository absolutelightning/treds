package radix_tree

import (
	"bytes"
)

// Tree implements an immutable radix tree. This can be treated as a
// Dictionary abstract data type. The main advantage over a standard
// hash map is prefix-based lookups and ordered iteration. The immutability
// means that it is safe to concurrently read from a Tree without any
// coordination.
type Tree struct {
	root *Node
	size int
}

// New returns an empty Tree
func New() *Tree {
	t := &Tree{
		root: &Node{},
	}
	return t
}

// Len is used to return the number of elements in the tree
func (t *Tree) Len() int {
	return t.size
}

// Txn is a transaction on the tree. This transaction is applied
// atomically and returns a new tree when committed. A transaction
// is not thread safe, and should only be used by a single goroutine.
type Txn struct {
	// root is the modified root for the transaction.
	root *Node

	// size tracks the size of the tree as it is modified during the
	// transaction.
	size int
}

// Txn starts a new transaction that can be used to mutate the tree
func (t *Tree) Txn() *Txn {
	txn := &Txn{
		root: t.root,
		size: t.size,
	}
	return txn
}

// Clone makes an independent copy of the transaction. The new transaction
// does not track any nodes and has TrackMutate turned off. The cloned transaction will contain any uncommitted writes in the original transaction but further mutations to either will be independent and result in different radix trees on Commit. A cloned transaction may be passed to another goroutine and mutated there independently however each transaction may only be mutated in a single thread.
func (t *Txn) Clone() *Txn {
	// reset the writable node cache to avoid leaking future writes into the clone
	txn := &Txn{
		root: t.root,
		size: t.size,
	}
	return txn
}

// Visit all the nodes in the tree under n, and add their mutateChannels to the transaction
// Returns the size of the subtree visited
func (t *Txn) trackChannelsAndCount(n *Node) int {
	// Count only leaf nodes
	leaves := 0
	if n.leaf != nil {
		leaves = 1
	}
	// Recurse on the children
	for _, e := range n.edges {
		leaves += t.trackChannelsAndCount(e.node)
	}
	return leaves
}

// mergeChild is called to collapse the given node with its child. This is only
// called when the given node is not a leaf and has a single edge.
func (t *Txn) mergeChild(n *Node) {
	// Mark the child node as being mutated since we are about to abandon
	// it. We don't need to mark the leaf since we are retaining it if it
	// is there.
	e := n.edges[0]
	child := e.node

	// Merge the nodes.
	n.prefix = concat(n.prefix, child.prefix)
	n.leaf = child.leaf
	n.minLeaf = child.leaf
	if len(child.edges) != 0 {
		n.edges = make([]edge, len(child.edges))
		copy(n.edges, child.edges)
	} else {
		n.edges = nil
	}
}

// insert does a recursive insertion
func (t *Txn) insert(n *Node, k, search []byte, v interface{}) (*Node, interface{}, bool) {
	// Handle key exhaustion
	if len(search) == 0 {
		var oldVal interface{}
		didUpdate := false
		if n.isLeaf() {
			oldVal = n.leaf.val
			didUpdate = true
		}

		n.leaf = &LeafNode{
			key: k,
			val: v,
		}
		n.computeLinks()
		return n, oldVal, didUpdate
	}

	// Look for the edge
	idx, child := n.getEdge(search[0])

	// No edge, create one
	if child == nil {
		leaf := &LeafNode{
			key: k,
			val: v,
		}
		e := edge{
			label: search[0],
			node: &Node{
				leaf:    leaf,
				minLeaf: leaf,
				maxLeaf: leaf,
				prefix:  search,
			},
		}
		n.addEdge(e)
		n.computeLinks()
		return n, nil, false
	}

	// Determine longest prefix of the search key on match
	commonPrefix := longestPrefix(search, child.prefix)
	if commonPrefix == len(child.prefix) {
		search = search[commonPrefix:]
		newChild, oldVal, didUpdate := t.insert(child, k, search, v)
		if newChild != nil {
			n.edges[idx].node = newChild
			n.computeLinks()
			return n, oldVal, didUpdate
		}
		return nil, oldVal, didUpdate
	}

	// Split the node
	splitNode := &Node{
		prefix: search[:commonPrefix],
	}
	n.replaceEdge(edge{
		label: search[0],
		node:  splitNode,
	})

	// Restore the existing child node
	splitNode.addEdge(edge{
		label: child.prefix[commonPrefix],
		node:  child,
	})
	child.prefix = child.prefix[commonPrefix:]

	// Create a new leaf node
	leaf := &LeafNode{
		key: k,
		val: v,
	}

	// If the new key is a subset, add to to this node
	search = search[commonPrefix:]
	if len(search) == 0 {
		splitNode.leaf = leaf
		splitNode.minLeaf = leaf
		splitNode.maxLeaf = leaf
		splitNode.computeLinks()
		return n, nil, false
	}

	// Create a new edge for the node
	splitNode.addEdge(edge{
		label: search[0],
		node: &Node{
			leaf:    leaf,
			minLeaf: leaf,
			maxLeaf: leaf,
			prefix:  search,
		},
	})
	splitNode.computeLinks()
	n.computeLinks()
	return n, nil, false
}

// delete does a recursive deletion
func (t *Txn) delete(parent, n *Node, search []byte) (*Node, *LeafNode) {
	// Check for key exhaustion
	if len(search) == 0 {
		if !n.isLeaf() {
			return nil, nil
		}
		// Copy the pointer in case we are in a transaction that already
		// modified this node since the node will be reused. Any changes
		// made to the node will not affect returning the original leaf
		// value.
		oldLeaf := n.leaf

		// Remove the leaf node
		n.leaf = nil
		n.minLeaf = nil
		n.maxLeaf = nil

		// Check if this node should be merged
		if n != t.root && len(n.edges) == 1 {
			t.mergeChild(n)
		}
		return n, oldLeaf
	}

	// Look for an edge
	label := search[0]
	idx, child := n.getEdge(label)
	if child == nil || !bytes.HasPrefix(search, child.prefix) {
		return nil, nil
	}

	// Consume the search prefix
	search = search[len(child.prefix):]
	newChild, leaf := t.delete(n, child, search)
	if newChild == nil {
		return nil, nil
	}

	// Delete the edge if the node has no edges
	if newChild.leaf == nil && len(newChild.edges) == 0 {
		n.delEdge(label)
		if n != t.root && len(n.edges) == 1 && !n.isLeaf() {
			t.mergeChild(n)
		}
	} else {
		n.edges[idx].node = newChild
	}
	n.computeLinks()
	return n, leaf
}

// delete does a recursive deletion
func (t *Txn) deletePrefix(parent, n *Node, search []byte) (*Node, int) {
	// Check for key exhaustion
	if len(search) == 0 {
		if n.isLeaf() {
			n.leaf = nil
		}
		n.edges = nil
		n.computeLinks()
		return n, t.trackChannelsAndCount(n)
	}

	// Look for an edge
	label := search[0]
	idx, child := n.getEdge(label)
	// We make sure that either the child node's prefix starts with the search term, or the search term starts with the child node's prefix
	// Need to do both so that we can delete prefixes that don't correspond to any node in the tree
	if child == nil || (!bytes.HasPrefix(child.prefix, search) && !bytes.HasPrefix(search, child.prefix)) {
		return nil, 0
	}

	// Consume the search prefix
	if len(child.prefix) > len(search) {
		search = []byte("")
	} else {
		search = search[len(child.prefix):]
	}
	newChild, numDeletions := t.deletePrefix(n, child, search)
	if newChild == nil {
		return nil, 0
	}
	// Copy this node. WATCH OUT - it's safe to pass "false" here because we
	// will only ADD a leaf via nc.mergeChild() if there isn't one due to
	// the !nc.isLeaf() check in the logic just below. This is pretty subtle,
	// so be careful if you change any of the logic here.

	// Delete the edge if the node has no edges
	if newChild.leaf == nil && len(newChild.edges) == 0 {
		n.delEdge(label)
		if n != t.root && len(n.edges) == 1 && !n.isLeaf() {
			t.mergeChild(n)
		}
	} else {
		n.edges[idx].node = newChild
	}
	n.computeLinks()
	return n, numDeletions
}

// Insert is used to add or update a given key. The return provides
// the previous value and a bool indicating if any was set.
func (t *Txn) Insert(k []byte, v interface{}) (interface{}, bool) {
	newRoot, oldVal, didUpdate := t.insert(t.root, k, k, v)
	if newRoot != nil {
		t.root = newRoot
	}
	if !didUpdate {
		t.size++
	}
	return oldVal, didUpdate
}

// Delete is used to delete a given key. Returns the old value if any,
// and a bool indicating if the key was set.
func (t *Txn) Delete(k []byte) (interface{}, bool) {
	newRoot, leaf := t.delete(nil, t.root, k)
	if newRoot != nil {
		t.root = newRoot
	}
	if leaf != nil {
		t.size--
		return leaf.val, true
	}
	return nil, false
}

// DeletePrefix is used to delete an entire subtree that matches the prefix
// This will delete all nodes under that prefix
func (t *Txn) DeletePrefix(prefix []byte) bool {
	newRoot, numDeletions := t.deletePrefix(nil, t.root, prefix)
	if newRoot != nil {
		t.root = newRoot
		t.size = t.size - numDeletions
		return true
	}
	return false

}

// Root returns the current root of the radix tree within this
// transaction. The root is not safe across insert and delete operations,
// but can be used to read the current state during a transaction.
func (t *Txn) Root() *Node {
	return t.root
}

// Get is used to lookup a specific key, returning
// the value and if it was found
func (t *Txn) Get(k []byte) (interface{}, bool) {
	return t.root.Get(k)
}

// Commit is used to finalize the transaction and return a new tree. If mutation
// tracking is turned on then notifications will also be issued.
func (t *Txn) Commit() *Tree {
	nt := t.CommitOnly()
	return nt
}

// CommitOnly is used to finalize the transaction and return a new tree, but
// does not issue any notifications until Notify is called.
func (t *Txn) CommitOnly() *Tree {
	nt := &Tree{t.root, t.size}
	return nt
}

// Insert is used to add or update a given key. The return provides
// the new tree, previous value and a bool indicating if any was set.
func (t *Tree) Insert(k []byte, v interface{}) (*Tree, interface{}, bool) {
	txn := t.Txn()
	old, ok := txn.Insert(k, v)
	return txn.Commit(), old, ok
}

// Delete is used to delete a given key. Returns the new tree,
// old value if any, and a bool indicating if the key was set.
func (t *Tree) Delete(k []byte) (*Tree, interface{}, bool) {
	txn := t.Txn()
	old, ok := txn.Delete(k)
	return txn.Commit(), old, ok
}

// DeletePrefix is used to delete all nodes starting with a given prefix. Returns the new tree,
// and a bool indicating if the prefix matched any nodes
func (t *Tree) DeletePrefix(k []byte) (*Tree, bool) {
	txn := t.Txn()
	ok := txn.DeletePrefix(k)
	return txn.Commit(), ok
}

// Root returns the root node of the tree which can be used for richer
// query operations.
func (t *Tree) Root() *Node {
	return t.root
}

// Get is used to lookup a specific key, returning
// the value and if it was found
func (t *Tree) Get(k []byte) (interface{}, bool) {
	return t.root.Get(k)
}

// longestPrefix finds the length of the shared prefix
// of two strings
func longestPrefix(k1, k2 []byte) int {
	max := len(k1)
	if l := len(k2); l < max {
		max = l
	}
	var i int
	for i = 0; i < max; i++ {
		if k1[i] != k2[i] {
			break
		}
	}
	return i
}

// concat two byte slices, returning a third new copy
func concat(a, b []byte) []byte {
	c := make([]byte, len(a)+len(b))
	copy(c, a)
	copy(c[len(a):], b)
	return c
}
