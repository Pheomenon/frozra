package persistence

import "fmt"

// indexer is a bst stored every table's minimum key and its corresponding file descriptor.
// when searching at l1 table it will check bst firstly then return the address of the table
// closest to the target key.

type indexer struct {
	root *node
}

type node struct {
	left, right *node
	minimumKey  uint32 // minimum key
	fd          uint32 // file descriptor
}

func NewIndexer() *indexer {
	return &indexer{}
}

func newNode(minimumKey, fd uint32) *node {
	return &node{
		left:       nil,
		right:      nil,
		minimumKey: minimumKey,
		fd:         fd,
	}
}

func (i *indexer) put(key, fd uint32) {
	if i.root == nil {
		i.root = newNode(key, fd)
		return
	}
	node := i.root
	for i.root != nil {
		if node.minimumKey < key {
			if node.right != nil {
				node = node.right
			} else {
				node.right = newNode(key, fd)
				return
			}
		} else if node.minimumKey > key {
			if node.left != nil {
				node = node.left
			} else {
				node.left = newNode(key, fd)
				return
			}
		} else {
			node.fd = fd
			return
		}
	}
}

func (i *node) delete(minimumKey uint32) *node {
	if i == nil {
		return nil
	}
	if i.minimumKey > minimumKey {
		i.left = i.left.delete(minimumKey)
	} else if i.minimumKey < minimumKey {
		i.right = i.right.delete(minimumKey)
	} else {
		if i.right == nil {
			return i.left
		}
		if i.left == nil {
			return i.right
		}
		tmp := i
		i = min(tmp.right)
		deleteMin(i.right)
		i.left = tmp.left
	}
	return i
}

func min(i *node) *node {
	if i.left == nil {
		return i
	}
	return min(i.left)
}

func deleteMin(i *node) *node {
	if i.left == nil {
		return i.right
	}
	i.left = deleteMin(i.left)
	return i
}

func (n *node) floor(minimumKey uint32) *node {
	if n == nil {
		return nil
	}
	if n.minimumKey == minimumKey {
		return n
	} else if n.minimumKey > minimumKey {
		return n.left.floor(minimumKey)
	}
	tmp := n.right.floor(minimumKey)
	if tmp != nil {
		return tmp
	} else {
		return n
	}
}

func (i *indexer) get(minimumKey uint32) uint32 {
	for i != nil {
		node := i.root
		if node.minimumKey == minimumKey {
			return node.fd
		} else if node.minimumKey < minimumKey {
			node = node.right
		} else if node.minimumKey > minimumKey {
			node = node.left
		}
	}
	panic(fmt.Sprintf("node: search in an empty node, get(%d)", minimumKey))
}

func (i *indexer) floor(minimumKey uint32) *node {
	return i.root.floor(minimumKey)
}

func (i *indexer) delete(minimumKey uint32) {
	i.root.delete(minimumKey)
}
