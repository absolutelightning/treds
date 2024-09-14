package radix

import "bytes"

// ReverseIterator is used to iterate over a set of nodes
// in reverse in-order
type ReverseIterator struct {
	i *Iterator
}

// NewReverseIterator returns a new ReverseIterator at a node
func NewReverseIterator(n *Node) *ReverseIterator {
	return &ReverseIterator{
		i: &Iterator{node: n},
	}
}

// SeekPrefixWatch is used to seek the iterator to a given prefix
// and returns the watch channel of the finest granularity
func (ri *ReverseIterator) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	return ri.i.SeekPrefixWatch(prefix)
}

// SeekPrefix is used to seek the iterator to a given prefix
func (ri *ReverseIterator) SeekPrefix(prefix []byte) {
	ri.i.SeekPrefixWatch(prefix)
}

// Previous returns the previous node in reverse order
func (ri *ReverseIterator) Previous() ([]byte, interface{}, bool) {
	// Initialize our stack if needed
	if ri.i.leafNode == nil && ri.i.node != nil {
		ri.i.leafNode = ri.i.node.maxLeaf
	}

	for ri.i.leafNode != nil {
		if bytes.HasPrefix(ri.i.leafNode.key, ri.i.key) {
			res := ri.i.leafNode
			ri.i.leafNode = ri.i.leafNode.GetPrevLeaf()
			if ri.i.leafNode == nil {
				ri.i.node = nil
			}
			return res.key, res.val, true
		} else {
			ri.i.leafNode = nil
			ri.i.node = nil
			break
		}
	}
	return nil, nil, false
}
