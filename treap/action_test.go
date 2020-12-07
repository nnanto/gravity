package treap

import (
	"testing"
)

func TestInsert(t *testing.T) {
	var r *Node = nil
	wrapper := func(totalNodes int, newFs func(int) *FreeSpace) func(t *testing.T) {
		return func(t *testing.T) {
			for i := 0; i < totalNodes; i++ {
				r = Insert(r, NewNode(newFs(i)))
			}
			VerifyHeapAndParentOrder(t, r)
			VerifyInOrder(t, r)
			//t.Logf("Height of tree: %v\n", height(r))
		}
	}

	t.Run("linear", wrapper(1000, func(i int) *FreeSpace {
		return &FreeSpace{Start: uint64(i + 2), End: uint64(i)}
	}))

	t.Run("overlapping", wrapper(1000, func(i int) *FreeSpace {
		return &FreeSpace{Start: uint64(i), End: uint64(i + 1)}
	}))

	t.Run("random", wrapper(1000, func(i int) *FreeSpace {
		return &FreeSpace{Start: uint64(i), End: uint64(i) + uint64(fastrandn(1000))}
	}))
}

func Benchmark_Insert(b *testing.B) {

	b.Run("Ordered", func(b *testing.B) {
		var r *Node = nil
		gap := 2
		for i := 0; i < b.N; i++ {
			r = Insert(r, NewNode(&FreeSpace{Start: uint64(i + gap), End: uint64(i + 1)}))
		}
		//fmt.Printf("Nodes: %v, Height: %v\n", b.N, height(r))
	})

	b.Run("Unordered", func(b *testing.B) {
		var r *Node = nil
		for i := 0; i < b.N; i++ {
			ran := i + int(fastrandn(uint32(b.N)))
			fs := &FreeSpace{Start: uint64(i + ran), End: uint64(i + ran + 10)}
			//fmt.Printf("RootFs: %v, FS: %v\n", fs)
			r = Insert(r, NewNode(fs))
		}
	})
}

func height(root *Node) int {
	if root == nil {
		return 0
	}

	left := height(root.left)
	right := height(root.right)
	if left < right {
		return 1 + right
	}
	return 1 + left
}
