package treap

import "fmt"

type FreeSpace struct {
	Start uint64 // Start position
	End   uint64 // End position
}

func (fs *FreeSpace) Size() uint64 {
	if fs.Start > fs.End {
		return 0
	}
	return fs.End - fs.Start + 1
}

func (fs *FreeSpace) String() string {
	return fmt.Sprintf("[%v:%v]", fs.Start, fs.End)
}
