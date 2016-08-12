package lmq

import (
	"encoding/binary"
	"strconv"
	"sync"
)

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}

func uInt64ToBytes(i uint64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func bytesToUInt64(buf []byte) uint64 {
	return binary.BigEndian.Uint64(buf)
}

func uInt64ToString(i uint64) string {
	return strconv.FormatUint(i, 10)
}
