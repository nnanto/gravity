package gravity

import (
	"github.com/stretchr/testify/require"
	"ohalloc/treap"
	"sync"
	"testing"
	"time"
)

// emptyLock provides an empty lock interface used by sync.Cond
type emptyLock struct {
}

func (l *emptyLock) Lock()   {}
func (l *emptyLock) Unlock() {}

func TestTreapSpaceAlg_Add(t *testing.T) {
	wrapper := func(testData [][]uint64, additionalChecks func(ts *freeSpaceManager)) func(t *testing.T) {
		return func(t *testing.T) {
			ts := newFSM()
			for _, d := range testData {
				_ = ts.add(&treap.FreeSpace{Start: d[0], End: d[1]})
			}
			treap.VerifyInOrder(t, ts.root)
			treap.VerifyHeapAndParentOrder(t, ts.root)
			verifyTotalFreeSpace(t, testData, ts)
			if additionalChecks != nil {
				additionalChecks(ts)
			}
		}
	}

	t.Run("no merge", wrapper(
		[][]uint64{{28, 30}, {2, 6}, {7, 12}, {15, 19}, {21, 26}, {41, 60}, {80, 100}},
		nil,
	))

	t.Run("simple merge", wrapper(
		[][]uint64{{28, 30}, {2, 6}, {7, 12}, {15, 20}, {21, 27}, {41, 60}, {80, 100}},
		nil,
	))

	t.Run("merge all", wrapper(
		[][]uint64{{20, 29}, {60, 69}, {0, 9}, {40, 49}, {30, 39}, {50, 59}, {10, 19}},
		func(ts *freeSpaceManager) {
			treap.VerifyLonelyNode(t, ts.root)
		},
	))

	t.Run("merge linear tree ", wrapper(
		[][]uint64{{60, 69}, {40, 49}, {20, 29}, {0, 9}, {30, 39}},
		func(ts *freeSpaceManager) {
			require.Equal(t, 3, treap.CountNodes(ts.root))
		},
	))

	t.Run("merge large tree", wrapper(
		[][]uint64{{40, 60}, {20, 25}, {80, 85}, {10, 13}, {33, 37}, {29, 31},
			{65, 69}, {95, 99}, {120, 122}, {75, 77},
			// merge items in left sub tree
			{26, 28}, {38, 39}, {14, 19},
			// merge items in right sub tree
			{78, 79}, {61, 64},
		},
		nil,
	))

}

func TestTreapdelete(t *testing.T) {
	var data = [][]uint64{{40, 60}, {20, 25}, {80, 85}, {10, 13}, {33, 37}, {29, 31},
		{65, 69}, {95, 99}, {120, 122}, {75, 77}}
	ts := newFSM()
	for _, d := range data {
		_ = ts.add(&treap.FreeSpace{Start: d[0], End: d[1]})
	}
	ch := make(chan []uint64)
	go func() {
		getDeletionOrder(data, ch)
		close(ch)
	}()
	dcount := len(data)
	for d := range ch {
		fs := &treap.FreeSpace{Start: d[0], End: d[1]}
		t.Logf("Deleting %vmap\n", fs)
		tn := treap.NewNode(fs)
		var dn *treap.Node
		ts.root, dn = treap.Remove(ts.root, tn)
		require.Equal(t, fs.String(), dn.String())
		treap.VerifyInOrder(t, ts.root)
		treap.VerifyHeapAndParentOrder(t, ts.root)
		dcount--
	}
	require.Equal(t, 0, dcount)
}

func TestTreapGet(t *testing.T) {
	tnodes := [][]uint64{{40, 60}, {20, 25}, {80, 85}, {10, 13}, {33, 37}, {29, 31},
		{65, 69}, {95, 99}, {120, 122}, {75, 77}}
	ts := newFSM()
	for _, tnode := range tnodes {
		ts.add(&treap.FreeSpace{Start: tnode[0], End: tnode[1]})
	}
	for _, tnode := range tnodes {
		fs, err := ts.poolExtract(tnode[1] - tnode[0])
		require.NoError(t, err)
		t.Logf("Extracted %vmap \n", fs)
		fs[0].Start = fs[0].End + 1
		go func() {
			time.Sleep(1 * time.Second)
			ts.poolPut(fs[0])
		}()

	}
	_, err := ts.poolExtract(1)
	require.Error(t, err)

}

func TestTreapPool(t *testing.T) {
	tnodes := [][]uint64{{40, 60}, {20, 25}, {80, 85}, {10, 13}, {33, 37}, {29, 31},
		{65, 69}, {95, 99}, {120, 122}, {75, 77}}
	ts := newFSM()
	for _, tnode := range tnodes {
		ts.add(&treap.FreeSpace{Start: tnode[0], End: tnode[1]})
	}
	requiredSpaces := []uint64{10, 5, 5}
	wg := sync.WaitGroup{}
	for _, requiredSpace := range requiredSpaces {
		wg.Add(1)
		go func(r uint64) {
			defer wg.Done()
			fss, err := ts.poolExtract(r)
			require.NoError(t, err)
			require.Len(t, fss, 1)
			fs := fss[0]
			require.GreaterOrEqual(t, fs.Size(), r)
			time.Sleep(150 * time.Millisecond)
			fs.Start += r
			ts.poolPut(fs)
		}(requiredSpace)
	}
	wg.Wait()
}

func TestTreapExtractMultiple(t *testing.T) {
	tnodes := [][]uint64{{40, 60}, {20, 25}, {80, 85}, {10, 13}, {33, 37}, {29, 31},
		{65, 69}, {95, 99}, {120, 122}, {75, 77}}
	ts := newFSM()
	for _, tnode := range tnodes {
		ts.add(&treap.FreeSpace{Start: tnode[0], End: tnode[1]})
	}
	reqSize := uint64(50)
	fss, err := ts.poolExtract(reqSize)
	require.NoError(t, err)
	totalSpaceExtracted := uint64(0)
	for _, space := range fss {
		totalSpaceExtracted += space.Size()
	}
	t.Logf("total space required: %vmap, extracted: %vmap [%vmap]\n", reqSize, totalSpaceExtracted, fss)
	require.GreaterOrEqual(t, totalSpaceExtracted, reqSize)
}

func TestFittingNeighbours(t *testing.T) {
	tnodes := [][]uint64{{40, 60}, {20, 25}, {80, 85}, {10, 13}, {33, 37}, {29, 31},
		{65, 69}, {95, 99}, {120, 122}, {75, 77}}
	ts := newFSM()
	for _, tnode := range tnodes {
		ts.add(&treap.FreeSpace{Start: tnode[0], End: tnode[1]})
	}
	var fss []*treap.FreeSpace
	var esize uint64
	fss, ts.root, esize = treap.GetFittingNeighbours(ts.root, ts.root, 60)
	require.Greater(t, esize, uint64(60))
	require.Greater(t, len(fss), 2)
	var lastFreeSpace *treap.FreeSpace
	for i, freeSpace := range fss {
		if i == 0 {
			lastFreeSpace = freeSpace
		} else {
			require.Greater(t, freeSpace.Start, lastFreeSpace.Start)
		}
	}
}

func getDeletionOrder(data [][]uint64, ch chan []uint64) {
	if len(data) == 0 {
		return
	}
	if len(data) == 1 {
		ch <- data[0]
		return
	}
	mid := len(data) / 2
	ch <- data[mid]
	getDeletionOrder(data[:mid], ch)
	getDeletionOrder(data[mid+1:], ch)
}

//func BenchmarkTreapSpaceAlg_Add(b *testing.B) {
//	b.StopTimer()
//	ts := newFSM()
//	s := 0
//	var fs []*FreeSpace
//	for i := 0; i < b.N; i++ {
//		r := rand.Intn(20)
//		s = s + r
//		e := s + r
//		s = e
//		fs = append(fs, &FreeSpace{Start: uint64(s), End: uint64(e)})
//	}
//	b.StartTimer()
//	for _, f := range fs {
//		_ = ts.add(f)
//	}
//	b.StopTimer()
//	b.Logf("Nodes: %vmap, Height: %vmap\n", b.N, ts.height(ts.root))
//}

func verifyTotalFreeSpace(t *testing.T, testData [][]uint64, ts *freeSpaceManager) {
	t.Helper()
	tfs := calcTFS(testData)
	require.Equal(t, tfs, ts.totalFreeSpace)
}

func calcTFS(testData [][]uint64) uint64 {
	tfs := uint64(0)
	for _, td := range testData {
		tfs += td[1] - td[0] + 1
	}
	return tfs
}
