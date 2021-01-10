package main

import (
	"fmt"
	"github.com/glycerine/offheap"
	"github.com/nnanto/gravity"
)

func main() {
	// create an offheap byte slice
	allocation := offheap.Malloc(1000, "")
	defer allocation.Free()

	// initialize gravity with offheap byte slice
	g, err := gravity.NewGravity(allocation.Mem)
	if err != nil {
		panic(err)
	}

	s := []string{"space", "gravity"}
	for i := 0; i < 10; i++ {
		k, _ := g.Write([]byte(s[i&1]))
		if i%3 == 0 {
			_ = g.Free(k)
		}
	}

	g.Iterate(func(pos uint64, key uint64, data []byte) bool {
		fmt.Printf("Found key: %v at pos: %v with data:%v\n", key, pos, string(data))
		return true
	})
}
