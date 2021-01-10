package gravity

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memoize random bytes of specified size
var rbytemem = make(map[int][]byte)
// lock on the rbytemem
var mu sync.RWMutex

func TestGravity_RWF(t *testing.T) {
	t.Run("ReadWrite Single", func(t *testing.T) {
		g, _ := NewGravity(make([]byte, 100))
		str := "hello"

		pos, err := g.Write([]byte(str))
		require.NoError(t, err)

		data, err := g.Read(pos)
		require.NoError(t, err)
		require.Equal(t, str, string(data))
	})

	t.Run("ReadWrite Many", func(t *testing.T) {
		g, _ := NewGravity(make([]byte, 5000))
		str := "hello"
		twrites := 100
		writeResult := make([]struct {
			pos  uint64
			data []byte
		}, twrites)

		for i := 0; i < twrites; i++ {
			data := []byte(str + string(rune(i)))
			pos, err := g.Write(data)
			require.NoError(t, err)
			writeResult[i].pos = pos
			writeResult[i].data = data
		}

		for _, wr := range writeResult {
			data, err := g.Read(wr.pos)
			require.NoError(t, err)
			require.Equal(t, wr.data, data)
		}
	})

	t.Run("WriteFree Single", func(t *testing.T) {
		g, _ := NewGravity(make([]byte, 5000))
		initsize := g.TotalFreeSpace()

		pos, err := g.Write([]byte("hello"))
		require.NoError(t, err)

		err = g.Free(pos)
		require.NoError(t, err)
		require.Equal(t, initsize, g.TotalFreeSpace())
	})

	t.Run("WriteFree Multi", func(t *testing.T) {
		g, _ := NewGravity(make([]byte, 6000))
		msgs := strings.Split("This is a sample test for multi writer and multifree", " ")
		var positions []uint64
		times := int64(10)
		for tt := int64(0); tt < times; tt++ {
			for i, msg := range msgs {
				p, e := g.Write([]byte(msg))
				if e != nil {
					t.Fatalf("Error while adding (%v-%v): %v\n", tt, i, e)
				}
				require.NoError(t, e)
				if i%5 == 0 || i%7 == 0 {
					positions = append(positions, p)
					require.NoError(t, g.Free(positions[0]))
					positions = positions[1:]
				}
			}
		}
		require.Error(t, g.Free(500))
	})

}

func TestGravity_VPos(t *testing.T) {
	inp := strings.Split("hello from the other side", " ")
	totalSpaceRequired := uint64(0)
	for _, s := range inp {
		totalSpaceRequired += uint64(len([]byte(s))) + headerLen + keyLen
	}
	g, _ := NewGravity(make([]byte, totalSpaceRequired))
	var positions []uint64
	k := 3
	for i, s := range inp {
		p, err := g.Write([]byte(s))
		require.NoError(t, err)
		if i >= k {
			positions = append(positions, p)
		}
	}
	for _, position := range positions {
		require.NoError(t, g.Free(position))
	}
	_, err := g.Write([]byte(inp[k] + inp[k+1]))
	require.NoError(t, err)
	_, err = g.Read(positions[1])
	require.EqualError(t, err, WrongReadPosition.Error())
}

func TestGravity_ExpandBasic(t *testing.T) {
	inp := strings.Split("a quick brown fox jumped over the lazy dog", " ")
	g := getGravity(inp)

	// add all inputs
	var positions []uint64
	for _, s := range inp {
		p, err := g.Write([]byte(s))
		require.NoError(t, err)
		positions = append(positions, p)
	}

	// Remove non-adjacent inputs
	removedPos := []int{0, 1, 4, 7}
	insertionString := ""
	for _, p := range removedPos {
		require.NoError(t, g.Free(positions[p]))
		insertionString += inp[p]
	}
	// add extra string to compensate for the header and keylength
	remainingSize := int(g.TotalFreeSpace()) - len(insertionString) - int(headerLen) - int(keyLen)
	insertionString += string(randBytes(remainingSize))

	// Try inserting data of size equal to removed size (exclude header)
	p, err := g.Write([]byte(insertionString))
	require.NoError(t, err)
	require.Equal(t, uint64(0), g.TotalFreeSpace())

	// Try reading the newly inserted data
	t.Logf("Reading items from removable list at pos: %v\n", p)
	d, err := g.Read(p)
	require.NoError(t, err)
	require.Equal(t, []byte(insertionString), d)

	// Try reading existing unremoved data
posloop:
	for i, position := range positions {
		for _, rp := range removedPos {
			if rp == i {
				continue posloop
			}
		}
		t.Logf("Reading item:%v at pos: %v \n", i, position)
		d, err := g.Read(position)
		require.NoError(t, err)
		require.Equal(t, []byte(inp[i]), d)
	}
}

func TestGravity_ExpandMove(t *testing.T) {
	inp := []string{"hello", "my", "world"}
	g := getGravity(inp)
	var positions []uint64
	for _, s := range inp {
		p, err := g.Write([]byte(s))
		require.NoError(t, err)
		positions = append(positions, p)
	}
	// freeing "my" of size 2
	require.NoError(t, g.Free(positions[1]))

	newData := []byte("four")
	p, err := g.Write(newData)
	require.NoError(t, err)
	d, err := g.Read(p)
	require.NoError(t, err)
	require.Equal(t, newData, d)
	require.Equal(t, uint64(0), g.TotalFreeSpace())
}

func TestGravity_SimpleRWParallel(t *testing.T) {
	inpString := "This is a sample sentence with at least ten words" +
		"This is a sample sentence with at least ten words This is a sample sentence with at least ten words " +
		"This is a sample sentence with at least ten words This is a sample sentence with at least ten words " +
		"This is a sample sentence with at least ten words This is a sample sentence with at least ten words "
	inp := strings.Split(inpString, " ")
	g := getGravity(inp)
	wg := sync.WaitGroup{}
	for i, s := range inp {
		if i > 30 {
			time.Sleep(100 * time.Millisecond)
		}
		key, err := g.Write([]byte(s))
		require.NoError(t, err)
		wg.Add(1)
		go func(k uint64, actual string) {
			defer wg.Done()
			if k%2 == 0 {
				d, err := g.Read(k)
				require.NoError(t, err)
				require.Equal(t, []byte(actual), d)
			}
			if k%3 == 0 {
				require.NoError(t, g.Free(k))
			}
		}(key, s)
	}
	wg.Wait()
}

func TestGravity_Bad(t *testing.T) {
	g, _ := NewGravity(make([]byte, 20))
	_, err := g.Write([]byte("hello"))
	require.Error(t, err)
	require.Equal(t, err.Error(), NotEnoughSpace.Error())

	_, err = g.Read(5)
	require.Error(t, err)
	require.Equal(t, err.Error(), WrongReadPosition.Error())

	_, err = g.Read(55)
	require.Error(t, err)
	require.Equal(t, err.Error(), WrongReadPosition.Error())

	err = g.Free(0)
	require.Error(t, err)
	require.Equal(t, err.Error(), WrongReadPosition.Error())
}

// Scenario:
// 1. Writer func writes to the memory until itemsToWrite is zero
// 2. FreeWriter frees every 100th data and writes a new data for every 300th entry
// 3. No error should happen during any write/free
func TestGravity_WriteFreeParallel(t *testing.T) {
	memSize := 160000
	g, _ := NewGravity(make([]byte, memSize))
	size := uint64(84) // so that each data block will be of size 100 (84 + 8 (headerLen) + 8 (keyLen))
	elementCount := memSize / int(size+headerLen+keyLen)
	itemsToWrite := uint64(elementCount)
	el := sync.NewCond(&emptyLock{})

	type D struct {
		key   uint64
		value []byte
	}
	// if w is not buffered, then there's a deadlock between processing of w and fw
	// deadlock between `fw <- key` and `w <- text`
	w := make(chan []byte, itemsToWrite+20)
	fw := make(chan D)

	writer := func() {
		for {
			select {
			case s, ok := <-w:
				if !ok {
					return
				}
				// write data
				key, err := g.Write(s)
				require.NoError(t, err)

				// notify freeWriter of the data
				fw <- D{key: key, value: s}

				// once everything has been written, signal & die
				if atomic.AddUint64(&itemsToWrite, ^uint64(0)) == 0 {
					el.Signal()
					return
				}

			}
		}
	}

	freeWriter := func() {
		for {
			select {
			case data, ok := <-fw:
				if !ok {
					return
				}
				k := data.key
				// Free every 100th data
				if k%100 == 0 {
					d, err := g.Read(k)
					require.NoError(t, err)
					require.Equal(t, data.value, d)
					require.NoError(t, g.Free(k))
				}

				// Write a new block for every 200th data. At this point we should potentially have multiple
				// freespaces extracted from pool i.e, extractedFreeSpaces could be > 0
				if k%200 == 0 {
					atomic.AddUint64(&itemsToWrite, 1)
					w <- randBytes(int(size))
				}
			}
		}
	}

	go writer()
	go freeWriter()

	// send data to writer
	for i := 0; i < elementCount; i++ {
		w <- randBytes(int(size))
	}
	// wait for signal & die
	el.Wait()
	// TFS should be 800 :  (160000/100) -> number of freed spaces. Half of these spaces are filled and so
	// half remains empty
	require.Equal(t, uint64(800), g.TotalFreeSpace())

}

// Scenario:
// 1. Write data and free every 10th data written (called as holes)
// 2. Create data two times the size of the hole and fill all the holes
func TestGravity_ReadExpandParallel(t *testing.T) {
	memSize := 160000
	g, _ := NewGravity(make([]byte, memSize))
	size := uint64(84)
	elementCount := memSize / int(size+headerLen+keyLen)

	holes := 0
	var holePositions []uint64
	var positions []uint64
	for elementCount > 0 {
		// write data
		k, err := g.Write(randBytes(int(size)))
		require.NoError(t, err)

		if elementCount%10 == 0 {
			// free every 10th data
			holePositions = append(holePositions, k)
			holes++
		} else {
			positions = append(positions, k)
		}

		elementCount--
	}

	for _, position := range holePositions {
		require.NoError(t, g.Free(position))
	}

	// create data twice the size and fill all the holes
	largeDataLength := (int(size) << 1) + int(headerLen+keyLen)
	// Since the data is twice the size, we need to iterate half the holes
	h2 := holes >> 1

	wg := sync.WaitGroup{}
	verifyRead := func(p uint64) {
		defer wg.Done()
		b, err := g.Read(p)
		require.NoError(t, err)
		require.Len(t, b, int(size))
	}
	rp := 0
	for i := 0; i < h2; i++ {
		wg.Add(1)
		// Write
		go func() {
			defer wg.Done()
			_, err := g.Write(randBytes(largeDataLength))
			if err != nil {
				t.Errorf(err.Error())
			}
		}()
		// verify read
		if rp < len(positions) {
			wg.Add(1)
			go verifyRead(positions[rp])
			rp++
		}
	}
	// verify remaining data
	for rp < len(positions) {
		wg.Add(1)
		go verifyRead(positions[rp])
		rp++
	}
	wg.Wait()
	require.Equal(t, uint64(0), g.TotalFreeSpace())
}

func randBytes(n int) []byte {
	mu.Lock()
	defer mu.Unlock()
	if x, ok := rbytemem[n]; ok {
		return x
	}
	d := make([]byte, n)
	rand.Read(d)
	rbytemem[n] = d
	return d
}

func getGravity(inp []string) *Gravity {
	totalSpaceRequired := uint64(0)
	for _, s := range inp {
		totalSpaceRequired += uint64(len([]byte(s))) + headerLen + keyLen
	}
	g, _ := NewGravity(make([]byte, totalSpaceRequired+2))
	return g
}

/**
******
Start of Benchmark
******
*/

func printHeap(b *testing.B) {
	b.Helper()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	b.Logf("HeapAlloc: %s\n", humanize.IBytes(ms.HeapAlloc))
}

func BenchmarkGravity_Write(b *testing.B) {
	wrapper := func(size uint64, dataCB func(size uint64) []byte) func(b *testing.B) {
		return func(b *testing.B) {
			s := uint64(100)
			if b.N > 100 {
				s = uint64(b.N)
				b.Logf("MMAP Allocation: %s\n", humanize.IBytes(uint64(s)))
			}

			g, _ := NewGravity(make([]byte, s))
			elementCount := b.N / int(size+headerLen+keyLen)

			b.ResetTimer()
			for i := 0; i < elementCount; i++ {
				_, _ = g.Write(dataCB(size))
			}
			printHeap(b)
		}
	}

	b.Run("small same data", wrapper(50, func(size uint64) []byte {
		return randBytes(int(size))
	}))

	b.Run("large same data", wrapper(5000, func(size uint64) []byte {
		return randBytes(int(size))
	}))

	b.Run("small different data", wrapper(50, func(size uint64) []byte {
		return randBytes(int(size))
	}))

	b.Run("large different data", wrapper(5000, func(size uint64) []byte {
		return randBytes(int(size))
	}))
}

func BenchmarkGravity_Free(b *testing.B) {
	memSize := b.N
	if memSize < 10000 {
		memSize = 10000
	}
	b.Logf("MMAP Allocation: %s\n", humanize.IBytes(uint64(memSize)))
	g, _ := NewGravity(make([]byte, memSize))
	size := uint64(64)
	elementCount := memSize / int(size+headerLen+keyLen)
	itemsWritten := elementCount
	var holePositions []uint64
	for elementCount > 0 {
		// write data
		k, _ := g.Write(randBytes(rand.Intn(int(size)) + 20))

		if elementCount%10 == 0 {
			// free every 10th data
			holePositions = append(holePositions, k)
		}

		elementCount--
	}
	fmt.Printf("Written #%v data of size:%v to memsize:%v\n", itemsWritten, size, memSize)
	b.ResetTimer()
	for i := range holePositions {
		g.Free(holePositions[i])
	}
}

func BenchmarkGravity_WriteWithFree(b *testing.B) {
	memSize := b.N * 10
	if memSize < 10000 {
		memSize = 10000
	}
	b.Logf("MMAP Allocation: %s\n", humanize.IBytes(uint64(memSize)))
	g, _ := NewGravity(make([]byte, memSize))
	size := uint64(64)
	elementCount := memSize / int(size+headerLen+keyLen)
	b.ResetTimer()
	for elementCount > 0 {
		// write data
		k, _ := g.Write(randBytes(rand.Intn(int(size)) + 20))

		if k > 5 && elementCount%10 == 0 {
			// free every 10th data
			require.NoError(b, g.Free(k-5))
			require.NoError(b, g.Free(k-3))
		}

		elementCount--
	}
}

func BenchmarkGravity_Expand(b *testing.B) {
	memSize := b.N
	if memSize < 10000 {
		memSize = 10000
	}
	b.Logf("MMAP Allocation: %s\n", humanize.IBytes(uint64(memSize)))
	g, _ := NewGravity(make([]byte, memSize))
	size := uint64(84)
	elementCount := memSize / int(size+headerLen+keyLen)
	var positions []uint64
	var holePositions []uint64
	for elementCount > 0 {
		// write data
		k, _ := g.Write(randBytes(int(size)))

		if elementCount%10 == 0 {
			// free every 10th data
			holePositions = append(holePositions, k)
		} else {
			positions = append(positions, k)
		}

		elementCount--
	}

	for _, hp := range holePositions {
		g.Free(hp)
	}
	// create data twice the size and fill all the holes
	largeDataLength := (int(size) * 2) + int(headerLen+keyLen)
	// Since the data is twice the size, we need to iterate half the holes
	h2 := len(holePositions) / 2
	//b.Logf("h2 : %v, holes: %v, elements: %v\n", h2, holes, memSize / int(size+headerLen+keyLen))
	b.ResetTimer()
	wc := writer(g, h2)
	//totalItems := int32(h2)
	for i := 0; i < h2; i++ {
		wc <- randBytes(largeDataLength)
	}
}

func writer(g *Gravity, maxCount int) chan []byte {
	writerWorkers := 10
	ch := make(chan []byte, writerWorkers)
	taskCount := uint64(0)
	for i := 0; i < writerWorkers; i++ {
		go func() {
			for {
				d, ok := <-ch
				if !ok {
					return
				}
				_, err := g.Write(d)
				if err != nil {
					fmt.Printf("err: %v\n", err)
				}
				if atomic.AddUint64(&taskCount, 1) == uint64(maxCount-1) {
					go close(ch)
					return
				}
			}
		}()
	}
	return ch
}
