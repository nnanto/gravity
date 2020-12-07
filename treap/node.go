package treap

const initG = -1

type Node struct {
	Fs *FreeSpace
	//Score       float64

	left, right *Node
	prev, next  *Node
	parent      *Node
}

func (n *Node) Size() uint64 {
	return n.Fs.Size()
}

// greater compares the start of two nodes
func (n *Node) greater(other *Node) bool {
	if n.Fs.Start == other.Fs.Start {
		return n.Fs.End > other.Fs.End
	}
	return n.Fs.Start > other.Fs.Start
}

func (n *Node) gravity() float64 {
	nextNode := n.next
	if nextNode == nil {
		return initG
	}
	d := float64(n.distance(nextNode))
	return float64(n.Fs.Size()) * float64(nextNode.Fs.Size()) / (d * d)
}

// distance between this node and the next
func (n *Node) distance(nextNode *Node) uint64 {
	return nextNode.Fs.Start - n.Fs.End
}

func (n *Node) String() string {
	return n.Fs.String()
}

// isOverlapping checks if the node's start or end is equal to other node's counterpart
func (n *Node) isOverlapping(other *Node) bool {
	if other == nil {
		return false
	}
	return n.Fs.Start == other.Fs.End+1 || other.Fs.Start == n.Fs.End+1
}

// mergeAndExpand expands the node's interval with overlapNode
func (n *Node) merge(overlapNode *Node) {

	if overlapNode.Fs.Start < n.Fs.Start {
		n.Fs.Start = overlapNode.Fs.Start
	}
	if overlapNode.Fs.End > n.Fs.End {
		n.Fs.End = overlapNode.Fs.End
	}
}

func (n *Node) Cleanup() {
	n.Fs = nil
	n.parent = nil
	n.prev = nil
	n.next = nil
	n.left = nil
	n.right = nil
}
