package treap

import (
	"log"
	_ "unsafe"
)

//go:linkname fastrandn runtime.fastrandn
func fastrandn(n uint32) uint32

func NewNode(fs *FreeSpace) *Node {
	return &Node{Fs: fs}
}

// Insert adds a new node if no overlapping nodes exists.
// overlapping nodes are merged with the first overlapping node encountered
func Insert(root *Node, nn *Node) *Node {
	if root == nil {
		return nn
	}

	if root.isOverlapping(nn) {
		mergeAndExpand(root, nn)
	} else if root.greater(nn) {
		root.left = Insert(root.left, nn)
		root.left.parent = root
		if root.left.next == nil {
			// left node should have the next pointed to the parent
			// so if its not there, then this is a new node
			setNextPrev(root.left, true)
		}
	} else {
		root.right = Insert(root.right, nn)
		root.right.parent = root
		if root.right.prev == nil {
			// right node should have the prev pointed to the parent
			// so if its not there, then this is a new node
			setNextPrev(root.right, false)
		}
	}

	// Insertion over so Heapify. These operations perform equivalent of `swim` in heap

	if root.left != nil && root.left.Size() > root.Size() {
		root = rotateRight(root)

	} else if root.right != nil && root.right.Size() > root.Size() {
		root = rotateLeft(root)
	}

	// Add in randomization for equal sized nodes, without this we'd get linear tree
	// Equality check is not added as part of previous heapification as it'd result in
	// nodes not being heapified correctly.
	// For ex: if root and root.left has same size and root.right is newly inserted
	// and if there was a check for root.size() * >= * root.left.size(), it'd be triggered and rotateRight would be
	// called completely ignoring root.right.size()

	if root.left != nil && root.left.Size() == root.Size() && fastrandn(10) < 5 {
		root = rotateRight(root)
	} else if root.right != nil && root.right.Size() == root.Size() && fastrandn(10) < 5 {
		root = rotateLeft(root)
	}

	return root
}

// Remove removes the node if present in the root's subtree and returns a new root and deleted node
func Remove(root *Node, node *Node) (*Node, *Node) {
	if root == nil {
		return nil, nil
	}
	var dn *Node
	if root.greater(node) {
		root.left, dn = Remove(root.left, node)
		// Update the parent of the left node
		if root.left != nil {
			root.left.parent = root
		}
		return root, dn
	} else if node.greater(root) {
		root.right, dn = Remove(root.right, node)
		// Update the parent of the right node
		if root.right != nil {
			root.right.parent = root
		}
		return root, dn
	}

	// if this is a leaf node
	if root.left == nil && root.right == nil {
		/*
			example:
			       6
			      /
				 4
				  \
				   5

			Deleting 5 should result in 4's next pointer to 6
		*/
		if root.prev != nil {
			root.prev.next = root.next
		}
		if root.next != nil {
			root.next.prev = root.prev
		}
		return nil, root
	}

	// Node with only right node
	if root.left == nil {
		/*
				example:
				    2
			         \
					 4
				      \
			          5

				Deleting 4 should result in 5's prev pointer to 2
		*/
		if root.prev != nil {
			root.prev.next = root.next
		}
		if root.next != nil {
			root.next.prev = root.prev
		}
		return root.right, root
	}

	// Node with only left node
	if root.right == nil {
		// Same as above example inverted
		if root.prev != nil {
			root.prev.next = root.next
		}
		if root.next != nil {
			root.next.prev = root.prev
		}
		return root.left, root
	}

	// For nodes with both left & right, rotate and move the node to the bottom (same as sink)
	if root.left.Size() > root.right.Size() {
		root = rotateRight(root)
		root, dn = Remove(root, root.right)
	} else {
		root = rotateLeft(root)
		root, dn = Remove(root, root.left)
	}
	return root, dn
}

// GreatestGravityNode returns a large gravity node that satisfies the given size
func GreatestGravityNode(root *Node, size uint64) *Node {
	crawl := root
	// wrapper around gravity
	g := func(a *Node) float64 {
		if a == nil {
			return -1.0
		}
		return a.gravity()
	}
	// returns maximum gravity node amongst the three
	max := func(a, b, c *Node) *Node {
		if g(a) > g(b) {
			if g(a) > g(c) {
				return a
			} else {
				return c
			}
		} else if g(b) > g(c) {
			return b
		}
		return c
	}

	// crawl down the root till we encounter a greater gravity node that satisfies the given size
	for crawl != nil {
		var l, r *Node
		if crawl.left != nil && crawl.left.Size() >= size {
			l = crawl.left
		}
		if crawl.right != nil && crawl.right.Size() >= size {
			r = crawl.right
		}
		if l == nil && r == nil {
			return crawl
		}
		next := max(crawl, l, r)
		if next == crawl {
			return crawl
		}
		crawl = next
	}
	return root
}

// GetFittingNeighbours returns list of freespaces (in sorted order) that together satisfies the given size,
// the new root and total space covered by the free space
func GetFittingNeighbours(root *Node, node *Node, size uint64) (fss []*FreeSpace, nr *Node, ts uint64) {

	ts = node.Size()
	fss = append(fss, node.Fs)
	nc := node.next
	pc := node.prev

	for ts < size {
		if nc != nil {
			ts += nc.Size()
			fss = append(fss, nc.Fs)
			nc = nc.next
		} else if pc != nil {
			ts += pc.Size()
			fss = append([]*FreeSpace{pc.Fs}, fss...)
			pc = pc.prev
		} else {
			// shouldn't come here
			panic("size not satisfied")
			//break
		}

	}

	// Remove all fetched freespaces
	nr = root
	var dn = &Node{}
	for _, fs := range fss {
		dn.Fs = fs
		nr, _ = Remove(nr, dn)
	}
	return
}

func Print(root *Node) {
	if root == nil {
		return
	}
	crawl, _ := minValueNode(root)
	fsCount := 0
	perLineLimit := 5
	for crawl != nil {
		fsCount++
		log.Printf("FS: %v (g: %v) ", crawl.String(), crawl.gravity())
		if fsCount%perLineLimit == 0 {
			log.Println("")
		}
		crawl = crawl.next
	}
	log.Printf("# Total Free Spaces Count = %v\n\n", fsCount)
}

// rotateLeft makes the right node the parent of current node(y)
func rotateLeft(y *Node) *Node {
	x := y.right
	y.right = x.left
	if x.left != nil {
		x.left.parent = y
	}
	x.left = y
	x.parent = y.parent
	y.parent = x
	return x
}

// rotateRight makes the left node the parent of current node(x)
func rotateRight(x *Node) *Node {
	y := x.left
	x.left = y.right
	if y.right != nil {
		y.right.parent = x
	}
	y.right = x
	y.parent = x.parent
	x.parent = y
	return y
}

// setNextPrev creates inorder predecessor and successor links to the new node (node) given
// parent node (parent) and whether if its a left/right child.
func setNextPrev(node *Node, left bool) {
	parent := node.parent
	if left {
		/*
				5
				 \
				  7
				 /
				6 (node)
			6.prev = 5
			6.next = 7
			7.prev = 6
			if..
				5.next = 6
		*/
		node.prev = parent.prev
		node.next = parent
		parent.prev = node
		if node.prev != nil {
			node.prev.next = node
		}
	} else {
		// same like above
		node.next = parent.next
		node.prev = parent
		parent.next = node
		if node.next != nil {
			node.next.prev = node
		}
	}
}

func mergeAndExpand(root *Node, nn *Node) {
	if root.next != nil && root.next.isOverlapping(nn) {
		// overlap with inorder successor
		// eg: root [1-3] , root.next [6-8] , nn [4-5]
		// deletes [6-8] and expands root to [1-8] and ignores [4-5]
		n := root.next
		oldNext := n.next
		Remove(n.parent, n)
		root.next = oldNext
		root.merge(n)
	} else if root.prev != nil && root.prev.isOverlapping(nn) {
		// overlap with inorder predecessor. Inverse example as above
		p := root.prev
		oldPrev := p.prev
		Remove(p.parent, p)
		root.prev = oldPrev
		root.merge(p)
	} else {
		// only overlaps with root, so just expand
		root.merge(nn)
	}
}

func minValueNode(node *Node) (*Node, *Node) {
	if node == nil {
		return nil, nil
	}
	crawl := node
	var parent *Node
	for crawl.left != nil {
		parent = crawl
		crawl = crawl.left
	}
	return crawl, parent
}

//func IsPartOfFreeSpace(root *Node, pos int64) bool {
//	if root == nil {
//		return false
//	}
//	if root.Fs.Start >= pos && root.Fs.End <= pos {
//		return true
//	}
//
//	if root.Fs.Start > pos {
//		return IsPartOfFreeSpace(root.left, pos)
//	}
//	return IsPartOfFreeSpace(root.right, pos)
//}

//func maxValueNode(node *Node) (*Node, *Node) {
//	crawl := node
//	var parent *Node
//	for crawl.right != nil {
//		parent = crawl
//		crawl = crawl.right
//	}
//	return crawl, parent
//}
//
// swim moves the node up if it has greater score
//func swim(node *Node) *Node {
//	if node == nil {
//		return nil
//	}
//	for node.parent != nil && node.parent.Size() <= node.Size() {
//		p := node.parent
//		if p.left == node {
//			node = rotateRight(p)
//		} else {
//			node = rotateLeft(p)
//		}
//		if node.parent != nil {
//
//			if node.parent.left == p {
//				node.parent.left = node
//			} else {
//				node.parent.right = node
//			}
//		}
//	}
//	return node
//}

//
//// sink moves the node down if it has lower score
//func sink(node *Node) {
//	greaterScoreNode := node
//	if node.left != nil && node.left.Score > greaterScoreNode.Score {
//		greaterScoreNode = node.left
//	}
//	if node.right != nil && node.right.Score > greaterScoreNode.Score {
//		greaterScoreNode = node.right
//	}
//
//	if greaterScoreNode == node {
//		return
//	}
//
//	if greaterScoreNode == node.left {
//		rotateRight(node)
//		sink(node)
//	} else {
//		rotateLeft(node)
//		sink(node)
//	}
//	return
//}
