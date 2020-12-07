package gravity

import (
	"errors"
	"log"
	"ohalloc/treap"
	"sync"
	"sync/atomic"
)

var (
	NotEnoughSpace    = errors.New("not enough space")
	ReadFromFreeSlot  = errors.New("read from free slot")
	NonFreeSlot       = errors.New("not a free slot")
)

// freeSpaceManager is a treap with freeSpace's start as key and the freeSpace's interval size as heap score
type freeSpaceManager struct {
	sync.Mutex
	plock          sync.RWMutex // lock for pooling
	pcond          *sync.Cond
	root           *treap.Node
	totalFreeSpace uint64
	inUseFsCount   int32 // number of nodes currently being used in the pool
	treapNodePool  sync.Pool
}

func newFSM() *freeSpaceManager {
	t := &freeSpaceManager{}
	t.pcond = sync.NewCond(&t.Mutex)
	t.treapNodePool = sync.Pool{
		New: func() interface{} { return new(treap.Node) },
	}
	return t
}

// add inserts or merges the provided free space
func (t *freeSpaceManager) add(fs *treap.FreeSpace) error {
	if fs.Size() <= 0 {
		return errors.New("empty freespace")
	}
	t.Lock()
	defer t.Unlock()
	nn := &treap.Node{}
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
	for (t.root == nil || t.root.Size() < expectedNodeSize) && t.inUseFsCount > 0 {
		t.pcond.Wait()
	}
	if t.root == nil || t.totalFreeSpace < size {
		t.Unlock()
		return nil, NotEnoughSpace
	}
	if t.root.Size() < expectedNodeSize {
		expectedNodeSize = 0
	}

	// Get max gravity node
	node := t.chooseFittingNode(expectedNodeSize)
	defer t.addToNodePool(node)
	fss, nr, extractedSize := treap.GetFittingNeighbours(t.root, node, size)
	// update the root
	t.root = nr
	t.totalFreeSpace -= extractedSize
	// inform that a fs has been pulled out
	t.inUseFsCount += 1
	t.Unlock()
	return fss, nil
}

func (t *freeSpaceManager) poolPut(fs *treap.FreeSpace) error {
	t.Lock()
	defer t.Unlock()
	t.inUseFsCount -= 1
	if t.inUseFsCount < 0 {
		return errors.New("pool put called without pool get")
	}
	defer t.pcond.Broadcast()
	if fs.Size() <= 0 {
		return errors.New("empty freespace")
	}
	nn := t.treapNodePool.Get().(*treap.Node)
	nn.Fs = fs
	t.root = treap.Insert(t.root, nn)
	atomic.AddUint64(&t.totalFreeSpace, fs.Size())
	return nil
}

// totalFreeSpaceSize provides the total free space after waiting for all free spaces
// to return to the pool
func (t *freeSpaceManager) totalFreeSpaceSize() uint64 {
	// obtain plock to wait for all free spaces to return back
	t.plock.Lock()
	defer t.plock.Unlock()
	return atomic.LoadUint64(&t.totalFreeSpace)
}

// adds the node back to the pool for reuse
func (t *freeSpaceManager) addToNodePool(node *treap.Node) {
	node.Cleanup()
	t.treapNodePool.Put(node)
}

// chooseFittingNode gets a node with greater gravity that satisfies the size
func (t *freeSpaceManager) chooseFittingNode(size uint64) *treap.Node {
	return treap.GreatestGravityNode(t.root, size)
}

// Waits for all the freespaces to return to the pool and then obtains a lock
func (t *freeSpaceManager) gLock() {
	t.plock.Lock() // wait for all items to get back to pool
	t.Lock()       // obtain lock on tree
}

// Unlocks the treap as well as the pool operation
func (t *freeSpaceManager) gUnlock() {
	t.Unlock()
	t.plock.Unlock()
}

func (t *freeSpaceManager) printLayout() {
	t.gLock()
	defer t.gUnlock()
	log.Printf("-----------Total Free Space (%vmap)---------------\n", t.totalFreeSpace)
	treap.Print(t.root)
}
