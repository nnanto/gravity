package gravity

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"ohalloc/treap"
	"sync"
	"sync/atomic"
)

type Gravity struct {
	sync.RWMutex
	mem  []byte            // Entire mem in bytes
	fsm  *freeSpaceManager // Manages the free space
	size uint64            // Total size of memory (same as len(mem))
	key  uint64            // Unique key for each data
	vmap *vmap             // Stores key to position of data
}

const (
	HeaderLen = uint64(8) // length of header to store the size of data
	KeyLen    = uint64(8) // Size of key for the data
)

var (
	WrongReadPosition = errors.New("wrong read position index")
)

const Debug = false

func logf(format string, args ...interface{}) {
	if Debug {
		log.Printf(format, args...)
	}
}

func NewGravity(mem []byte) (*Gravity, error) {
	size := uint64(len(mem))
	if size <= HeaderLen+KeyLen {
		return nil, errors.New("input byte too small")
	}

	//mmap := offheap.Malloc(int64(size), "")
	g := &Gravity{
		mem:  mem,
		fsm:  newFSM(),
		size: size,
		vmap: newShardedStore(),
		key:  uint64(1),
	}
	err := g.fsm.add(&treap.FreeSpace{Start: 0, End: size - 1})
	return g, err
}

// getKey increments the key atomically and returns the value
func (g *Gravity) getKey() uint64 {
	return atomic.AddUint64(&g.key, 1)
}

// Writes data to the memory and returns a key.
// The key acts as a reference to read the data
func (g *Gravity) Write(data []byte) (key uint64, err error) {
	key = g.getKey()
	err = g.write(data, key)
	return
}

func (g *Gravity) write(data []byte, k uint64) error {
	// get data size
	dl := uint64(len(data))
	totalLen := HeaderLen + KeyLen + dl

	// try to fetch freespace for size
	fss, err := g.fsm.poolExtract(totalLen)
	if err != nil {
		return err
	}

	// merge all freespaces to satisfy the data size
	fs := g.merge(fss)
	logf("Trying to write into fs: %vmap\n", fs)

	// remember to put the freespace back to the pool
	defer func() {
		err := g.fsm.poolPut(fs)
		if err != nil {
			logf("Error while adding to pool: %vmap\n", err)
		}
	}()

	// write to the memory
	npos := fs.Start
	err = g.writeAt(npos, data, k)
	if err != nil {
		return err
	}
	logf("Written into pos: %vmap\n", npos)

	// store virtual position
	g.vmap.store(k, npos)

	fs.Start += totalLen
	return nil
}

// Reads the value stored in the position corresponding to the key
func (g *Gravity) Read(key uint64) ([]byte, error) {
	g.RLock()
	defer g.RUnlock()
	pos, err := g.loadFromVPos(key)
	if err != nil {
		return nil, err
	}
	if pos >= g.size {
		return nil, errors.New("out of bound")
	}
	dl := binary.LittleEndian.Uint64(g.mem[pos : pos+HeaderLen])
	pos += HeaderLen + KeyLen
	b := make([]byte, dl)
	n := copy(b, g.mem[pos:pos+dl])
	if n != int(dl) {
		return nil, errors.New(fmt.Sprintf("expected to write %vmap but wrote %vmap", dl, n))
	}
	return b, nil
}

// Frees the memory held by the data pointed by key
func (g *Gravity) Free(key uint64) error {
	g.RLock()

	pos, err := g.loadAndDeleteFromVPos(key)
	if err != nil {
		g.RUnlock()
		return err
	}

	dl := binary.LittleEndian.Uint64(g.mem[pos : pos+HeaderLen])
	g.RUnlock()

	totalDataSize := HeaderLen + KeyLen + uint64(dl)
	return g.fsm.add(&treap.FreeSpace{Start: pos, End: pos + totalDataSize - 1})
}

// TotalFreeSpace indicates the remaining free space available
func (g *Gravity) TotalFreeSpace() uint64 {
	return g.fsm.totalFreeSpaceSize()
}

// merge joins multiple freespaces to form a single large free space.
// During the process, data is moved around and vmap is updated.
func (g *Gravity) merge(fss []*treap.FreeSpace) *treap.FreeSpace {
	g.Lock()
	defer g.Unlock()
	for i := 0; i < len(fss)-1; i++ {
		fs := fss[i]
		nfs := fss[i+1]
		// eg. f[2-8]--d[9-12]--f[13-17]
		// after shifting d[9-12] to the left
		// d[2-5]-f[6-17] : Here 6 is destEnd
		destEnd := fs.Start + (nfs.Start - fs.End - 1) // start of free slot + size of the data
		g.readAndShift(fs.End+1, nfs.Start, fs.Start, destEnd)
		nfs.Start = destEnd
	}

	return fss[len(fss)-1]
}

// Writes the data at given position and returns the key
func (g *Gravity) writeAt(pos uint64, data []byte, k uint64) error {
	dl := uint64(len(data))
	binary.LittleEndian.PutUint64(g.mem[pos:pos+HeaderLen], dl)
	pos += HeaderLen
	binary.LittleEndian.PutUint64(g.mem[pos:pos+KeyLen], uint64(k))
	pos += KeyLen
	n := copy(g.mem[pos:pos+uint64(dl)], data)
	if n != len(data) {
		return errors.New(fmt.Sprintf("expected to write %vmap but wrote %vmap", dl, n))
	}
	return nil
}

// readAndShift reads all the data from srcStart:srcEnd and moves them to dstStart:dstEnd
func (g *Gravity) readAndShift(srcStart uint64, srcEnd uint64, dstStart uint64, dstEnd uint64) {
	// update existing data pos
	runningDataLength := uint64(0)
	start := srcStart

	for start < srcEnd {
		if start+HeaderLen+KeyLen > g.size {
			panic("Trying to move src beyond size")
		}
		dl := binary.LittleEndian.Uint64(g.mem[start : start+HeaderLen])
		// rewire key position
		key := binary.LittleEndian.Uint64(g.mem[start+HeaderLen : start+HeaderLen+KeyLen])
		g.vmap.store(uint64(key), dstStart+runningDataLength)

		runningDataLength += uint64(dl) + HeaderLen + KeyLen
		start += uint64(dl) + HeaderLen + KeyLen
	}
	copy(g.mem[dstStart:dstEnd], g.mem[srcStart:srcEnd])
}

func (g *Gravity) loadFromVPos(key uint64) (uint64, error) {
	pos, ok := g.vmap.load(key)
	if !ok {
		return 0, WrongReadPosition
	}
	return pos, nil
}

func (g *Gravity) loadAndDeleteFromVPos(key uint64) (uint64, error) {
	pos, ok := g.vmap.loadAndDelete(key)
	if !ok {
		return 0, WrongReadPosition
	}
	return pos, nil
}
