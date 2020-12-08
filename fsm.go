package gravity

import (
	"errors"
	"log"
	"ohalloc/treap"
	"sync"
	"sync/atomic"
)

var (
	NotEnoughSpace = errors.New("not enough space")
	illegalPoolPut = errors.New("pool put called without pool get")
)


// freeSpaceManager is a treap with freeSpace's start as key and the freeSpace's interval size as heap score
type freeSpaceManager struct {
	sync.Mutex
	pcond               *sync.Cond // condition to notify freespaces returned to the pool
	root                *treap.Node
	totalFreeSpace      uint64
	extractedFreeSpaces int32     // number of freespaces currently being extracted from the pool
}

func newFSM() *freeSpaceManager {
	t := &freeSpaceManager{}
	t.pcond = sync.NewCond(&t.Mutex)
	return t
}

// add inserts or merges the provided free space
func (t *freeSpaceManager) add(fs *treap.FreeSpace) error {
	if fs.Size() <= 0 {
		return errors.New("empty freespace")
	}
	t.Lock()
	defer t.Unlock()
	nn := treap.NodePool.Get()
	nn.Fs = fs
	oldRoot := t.root
	t.root = treap.Insert(t.root, nn)
	t.totalFreeSpace += fs.Size()
	if oldRoot != t.root {
		// Broadcast is still called to all goroutines waiting to check if the root has been updated
		t.pcond.Broadcast()
	}
	return nil
}

func (t *freeSpaceManager) poolExtract(size uint64) ([]*treap.FreeSpace, error) {
	t.Lock()

	expectedNodeSize := size
	// wait till the root size is not met and there are in-use free space
	for (t.root == nil || t.root.Size() < expectedNodeSize) && t.extractedFreeSpaces > 0 {
		t.pcond.Wait()
	}
	// At this point totalFreeSpace contains all the items
	if t.root == nil || t.totalFreeSpace < size {
		t.Unlock()
		return nil, NotEnoughSpace
	}
	if t.root.Size() < expectedNodeSize {
		expectedNodeSize = 0
	}

	// Get max gravity node
	node := treap.GreatestGravityNode(t.root, expectedNodeSize)
	defer treap.NodePool.Put(node)
	fss, nr, extractedSize := treap.GetFittingNeighbours(t.root, node, size)
	// update the root
	t.root = nr
	t.totalFreeSpace -= extractedSize
	// inform that a fs has been pulled out
	t.extractedFreeSpaces += 1
	t.Unlock()
	return fss, nil
}

func (t *freeSpaceManager) poolPut(fs *treap.FreeSpace) error {
	t.Lock()
	defer t.Unlock()
	t.extractedFreeSpaces -= 1
	if t.extractedFreeSpaces < 0 {
		return illegalPoolPut
	}
	defer t.pcond.Broadcast()
	if fs.Size() <= 0 {
		return nil
	}
	nn := treap.NodePool.Get()
	nn.Fs = fs
	t.root = treap.Insert(t.root, nn)
	atomic.AddUint64(&t.totalFreeSpace, fs.Size())
	return nil
}

func (t *freeSpaceManager) waitForExtractedFreeSpaces() {
	for t.extractedFreeSpaces > 0 {
		t.pcond.Wait()
	}
}

// totalFreeSpaceSize provides the total free space after waiting for all free spaces
// to return to the pool
func (t *freeSpaceManager) totalFreeSpaceSize() uint64 {
	t.Lock()
	defer t.Unlock()
	t.waitForExtractedFreeSpaces()
	return atomic.LoadUint64(&t.totalFreeSpace)
}

func (t *freeSpaceManager) printLayout() {

	log.Printf("-----------Total Free Space (%vmap)---------------\n", t.totalFreeSpace)
	treap.Print(t.root)
}
