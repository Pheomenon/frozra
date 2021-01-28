package persistence

import "xonlab.com/frozra/v1/persistence/util"

// bst uses to find the maximum lower range of incoming key
type node struct {
	root       *node
	left       *node
	right      *node
	lowerRange uint32
	index      []uint32
}

func (n *node) put(lowerRange, index uint32) {
	for n != nil {
		if n.lowerRange == lowerRange {
			n.index = append(n.index, index)
			return
		} else if n.lowerRange > lowerRange {
			if n.left != nil {
				n = n.left
			} else {
				n.left = &node{
					left:       nil,
					right:      nil,
					root:       n,
					index:      []uint32{index},
					lowerRange: lowerRange,
				}
				return
			}
		} else {
			if n.right != nil {
				n = n.right
			} else {
				n.right = &node{
					left:       nil,
					right:      nil,
					root:       n,
					index:      []uint32{index},
					lowerRange: lowerRange,
				}
				break
			}
		}
	}
}

func (n *node) largestLowerRange(r uint32) *node {
	if n.lowerRange < r {
		if n.right != nil {
			return n.right.largestLowerRange(r)
		}
	}
	if n.lowerRange > r {
		if n.left != nil {
			return n.left.largestLowerRange(r)
		}
	}
	if n.lowerRange > r {
		return nil
	}
	return n
}

func (n *node) deleteTable(index uint32) {
	i, ok := util.InArray(n.index, index)
	if ok {
		n.index[i] = n.index[len(n.index)-1]
		n.index = n.index[:len(n.index)-1]
		if len(n.index) != 0 {
			return
		}
		if n.right != nil {
			n = n.right
			return
		}
		n = n.left
		return
	}

	if n.right != nil {
		n.right.deleteTable(index)
	}
	if n.left != nil {
		n.left.deleteTable(index)
	}
}

func (n *node) rootNode() *node {
	return n.root
}

type tree struct {
	root *node
}

func NewTree() *tree {
	return &tree{}
}

func (t *tree) put(lowerRange, index uint32) {
	if t.root == nil {
		t.root = &node{
			lowerRange: lowerRange,
			index:      []uint32{index},
			left:       nil,
			right:      nil,
			root:       t.root,
		}
		return
	}
	t.root.put(lowerRange, index)
}

func (t *tree) deleteTable(index uint32) {
	i, ok := util.InArray(t.root.index, index)
	if ok {
		t.root.index[i] = t.root.index[len(t.root.index)-1]
		t.root.index = t.root.index[:len(t.root.index)-1]
		if len(t.root.index) != 0 {
			return
		}
		if t.root.right != nil {
			t.root = t.root.right
			return
		}
		t.root = t.root.left
		return
	}
	if t.root.right != nil {
		t.root.right.deleteTable(index)
	}
	if t.root.left != nil {
		t.root.left.deleteTable(index)
	}
}

func (t *tree) largestLowerRange(r uint32) *node {
	if t.root == nil {
		return nil
	}
	if t.root.lowerRange < r {
		if t.root.right != nil {
			n := t.root.right.largestLowerRange(r)
			if n != nil {
				return n
			}
		}
	}
	if t.root.lowerRange > r {
		if t.root.left != nil {
			return t.root.left.largestLowerRange(r)
		}
	}
	if t.root.lowerRange > r {
		return nil
	}
	return t.root
}

func (t *tree) allLargestRange(r uint32) []*node {
	nodes := []*node{}
	for {
		n := t.largestLowerRange(r)
		if n == nil {
			break
		}
		nodes = append(nodes, n)
		r = n.lowerRange - 1
	}
	return nodes
}
