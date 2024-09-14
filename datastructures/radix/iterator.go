package radix

import (
	"bytes"
	"regexp"
)

// Iterator is used to iterate over a set of nodes
// in pre-order
type Iterator struct {
	node           *Node
	stack          []edges
	leafNode       *LeafNode
	key            []byte
	seekLowerBound bool
	patternMatch   bool
	pattern        string
}

func (i *Iterator) PatternMatch(regex string) {
	i.patternMatch = true
	i.pattern = regex
}

// SeekPrefixWatch is used to seek the iterator to a given prefix
// and returns the watch channel of the finest granularity
func (i *Iterator) SeekPrefixWatch(prefix []byte) (watch <-chan struct{}) {
	// Wipe the stack
	i.seekLowerBound = false
	i.stack = nil
	i.key = prefix
	n := i.node
	search := prefix
	for {
		// Check for key exhaustion
		if len(search) == 0 {
			i.node = n
			return
		}

		// Look for an edge
		_, n = n.getEdge(search[0])
		if n == nil {
			i.node = nil
			return
		}

		// Consume the search prefix
		if bytes.HasPrefix(search, n.prefix) {
			search = search[len(n.prefix):]

		} else if bytes.HasPrefix(n.prefix, search) {
			i.node = n
			return
		} else {
			i.node = nil
			return
		}
	}
}

// SeekPrefix is used to seek the iterator to a given prefix
func (i *Iterator) SeekPrefix(prefix []byte) {
	i.SeekPrefixWatch(prefix)
}

// Next returns the next node in order
func (i *Iterator) Next() ([]byte, interface{}, bool) {

	var zero interface{}

	if i.node != nil && i.leafNode == nil {
		i.leafNode, _ = i.node.MinimumLeaf()
	}

	if i.patternMatch {

		for i.leafNode != nil {
			matched := true
			if len(i.pattern) > 0 {
				matched, _ = regexp.MatchString(i.pattern, string(i.leafNode.key))
			}
			if i.leafNode != nil && matched {
				res := i.leafNode
				i.leafNode = i.leafNode.GetNextLeaf()
				if i.leafNode == nil {
					i.node = nil
				}
				return res.key, res.val, true
			} else {
				i.leafNode = i.leafNode.GetNextLeaf()
				if i.leafNode == nil {
					i.node = nil
				}
			}
		}

	} else {

		for i.leafNode != nil {
			if bytes.HasPrefix(i.leafNode.key, i.key) {
				res := i.leafNode
				i.leafNode = i.leafNode.GetNextLeaf()
				if i.leafNode == nil {
					i.node = nil
				}
				return res.key, res.val, true
			} else {
				i.leafNode = nil
				i.node = nil
				break
			}
		}
	}

	i.leafNode = nil
	i.node = nil

	return nil, zero, false
}
