# Gravity

Given a large byte array (consider as memory), Gravity helps in writing, reading and freeing of data onto the provided memory in a thread-safe way.

## Why would I need this?

The main purpose of Gravity is to help save data off heap. Providing `[]byte` from memory mapped region to Gravity will allow it to save data out of heap.

The [example](https://github.com/nnanto/gravity/blob/main/examples/ohalloc/main.go) uses https://github.com/glycerine/offheap to get byte slice of a memory
mapped region

```go
// create an offheap byte slice
allocation := offheap.Malloc(1000, "")
defer allocation.Free()

// initialize gravity with offheap byte slice
g, err := gravity.NewGravity(allocation.Mem)

// writes hello off heap
_, _ := g.Write([]byte("hello"))
```

## Why the name Gravity?
Long story short, when writing data to the memory, free spaces with higher gravity is preferred. This tends to help merge freespaces to
form bigger freespaces. Gravity of free space is defined as

<code>size(fs<sub>i</sub>) * size(fs<sub>i+1</sub>) / distance<sup>2</sup>(fs<sub>i</sub>, fs<sub>i+1</sub>)</code>

where <code>size(fs<sub>i</sub>)</code> is the size of i-th free space

[Read this post for more info](https://nnanto.medium.com/gravity-the-allocator-d443f970123e)

### Future Ideas

1. Try writing an in-memory cache on top of gravity.
