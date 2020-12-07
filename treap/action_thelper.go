package treap

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func VerifyLonelyNode(t *testing.T, node *Node) {
	t.Helper()

	require.Nil(t, node.left)
	require.Nil(t, node.right)
	require.Nil(t, node.prev)
	require.Nil(t, node.next)
}

func VerifyInOrder(t *testing.T, root *Node) {
	if root == nil {
		return
	}
	start, _ := minValueNode(root)
	var end *Node
	inorderCh := scanInorder(root)
	for fs := range inorderCh {
		require.Equal(t, fs, start)
		// check previous pointers
		require.Equal(t, end, start.prev)
		end = start
		start = start.next
	}
}

func scanInorder(root *Node) <-chan *Node {
	ch := make(chan *Node)

	go func() {
		scanInorderCh(root, ch)
		close(ch)
	}()

	return ch
}

func scanInorderCh(root *Node, ch chan *Node) {
	if root == nil {
		return
	}
	scanInorderCh(root.left, ch)
	ch <- root
	scanInorderCh(root.right, ch)
}

func VerifyHeapAndParentOrder(t *testing.T, root *Node) {
	t.Helper()

	if root == nil {
		return
	}
	var nodes []*Node
	nodes = append(nodes, root)
	l := 0
	//t.Logf("--------------------------------\n")
	for len(nodes) != 0 {
		l++
		//t.Logf("Level (%v): %v\n", l, nodes)
		var temp []*Node
		for _, n := range nodes {
			if n.left != nil {
				require.GreaterOrEqual(t, n.Size(), n.left.Size())
				require.Equal(t, n, n.left.parent)
				temp = append(temp, n.left)
			}
			if n.right != nil {
				require.GreaterOrEqual(t, n.Size(), n.right.Size())
				require.Equal(t, n, n.right.parent)
				temp = append(temp, n.right)
			}
		}
		nodes = temp
	}
}

func CountNodes(node *Node) int {
	if node == nil {
		return 0
	}

	return 1 + CountNodes(node.left) + CountNodes(node.right)
}
