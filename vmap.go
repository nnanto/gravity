package gravity

// maxBucket to shard the keys. should be a power of 2
const maxBucket = 0x20

type shard struct {
	m map[uint64]uint64
}

type vmap struct {
	bucket map[int]*shard
}

func newShardedStore() *vmap {
	ss := &vmap{
		bucket: make(map[int]*shard, maxBucket),
	}
	for i := 0; i < maxBucket; i++ {
		ss.bucket[i] = &shard{m: make(map[uint64]uint64)}
	}
	return ss
}


func bucketIdx(key uint64) int {
	return int(key & uint64(maxBucket-1))
}

func (v *vmap) store(key uint64, value uint64) {
	index := bucketIdx(key)
	if silo, ok := v.bucket[index]; ok {
		silo.m[key] = value
	}
}


func (v *vmap) load(key uint64) (uint64, bool) {
	index := bucketIdx(key)
	if silo, ok := v.bucket[index]; ok {
		val, ok := silo.m[key]
		return val, ok
	}
	return 0, false
}

func (v *vmap) loadAndDelete(key uint64) (uint64, bool) {
	index := bucketIdx(key)
	if silo, ok := v.bucket[index]; ok {
		val, ok := silo.m[key]
		if ok {
			delete(silo.m, key)
		}
		return val, ok
	}
	return 0, false
}
