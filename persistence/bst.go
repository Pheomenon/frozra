package persistence

// bst stored every table's minimum key and its corresponding file name.
// when searching in l0 or l1 table it will check bst firstly then return
// the address of the table closest to the target key.
type node struct {
	root       *node
	left       *node
	right      *node
	minimumKey uint32 // minimum key
	fd         uint32 // file descriptor
}

func (n *node) put(lowerRange, fd uint32) {
	for n != nil {
		if n.minimumKey == lowerRange {
			n.fd = fd
			return
		} else if n.minimumKey > lowerRange {
			if n.left != nil {
				n = n.left
			} else {
				n.left = &node{
					left:       nil,
					right:      nil,
					root:       n,
					fd:         fd,
					minimumKey: lowerRange,
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
					fd:         fd,
					minimumKey: lowerRange,
				}
				break
			}
		}
	}
}

func (n *node) largestLowerRange(r uint32) *node {
	if n.minimumKey < r {
		if n.right != nil {
			return n.right.largestLowerRange(r)
		}
	}
	if n.minimumKey > r {
		if n.left != nil {
			return n.left.largestLowerRange(r)
		}
	}
	if n.minimumKey > r {
		return nil
	}
	return n
}

func (n *node) deleteTable(index uint32) {
	//TODO:
	//i, ok := util.InArray(n.fd, index)
	//if ok {
	//	n.fd[i] = n.fd[len(n.fd)-1]
	//	n.fd = n.fd[:len(n.fd)-1]
	//	if len(n.fd) != 0 {
	//		return
	//	}
	//	if n.right != nil {
	//		n = n.right
	//		return
	//	}
	//	n = n.left
	//	return
	//}

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
	//TODO:
	//if t.root == nil {
	//	t.root = &node{
	//		minimumKey: lowerRange,
	//		fd:         []uint32{index},
	//		left:       nil,
	//		right:      nil,
	//		root:       t.root,
	//	}
	//	return
	//}
	t.root.put(lowerRange, index)
}

func (t *tree) deleteTable(index uint32) {
	//TODO:
	//i, ok := util.InArray(t.root.fd, index)
	//if ok {
	//	t.root.fd[i] = t.root.fd[len(t.root.fd)-1]
	//	t.root.fd = t.root.fd[:len(t.root.fd)-1]
	//	if len(t.root.fd) != 0 {
	//		return
	//	}
	//	if t.root.right != nil {
	//		t.root = t.root.right
	//		return
	//	}
	//	t.root = t.root.left
	//	return
	//}
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
	if t.root.minimumKey < r {
		if t.root.right != nil {
			n := t.root.right.largestLowerRange(r)
			if n != nil {
				return n
			}
		}
	}
	if t.root.minimumKey > r {
		if t.root.left != nil {
			return t.root.left.largestLowerRange(r)
		}
	}
	if t.root.minimumKey > r {
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
		r = n.minimumKey - 1
	}
	return nodes
}
