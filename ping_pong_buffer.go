package lmq

import (
	"sync"
	"time"
)

// PingPongBuffer keep a pair of ping/pong buffers, one full will trigger
// flush (in another goroutine) and switch to the other one to keep receive data.
type PingPongBuffer struct {
	cache0       []*Message
	cache1       []*Message
	currentCache *[]*Message
	sync.Mutex

	flushThreshold int
	bgFlush        chan bool
	flushInterval  time.Duration

	exitChan chan int

	handler func(msgs []*Message)
}

// NewPingPongBuffer creates a new PingPongBuffer using the given option.
// exitChan: when close, notify flush goroutine to exit.
// size: set ping pong buffer internal cache size.
// handler: when cache full, flush goroutine will push data to handler.
func NewPingPongBuffer(exitChan <-chan struct{}, size int, flushInterval time.Duration, handler func(msgs []*Message)) *PingPongBuffer {
	ppb := &PingPongBuffer{
		cache0:         make([]*Message, 0, size),
		cache1:         make([]*Message, 0, size),
		flushThreshold: size,
		bgFlush:        make(chan bool),
		flushInterval:  flushInterval,
		handler:        handler,
	}
	ppb.currentCache = &ppb.cache0

	return ppb
}

// Put receive msg to internal cache. when full,
// will notify flush goroutine to push data to hadnler,
// and switch to another cache to keep on receiving.
func (ppb *PingPongBuffer) Put(msg *Message) {
	ppb.Lock()
	defer ppb.Unlock()
	*ppb.currentCache = append(*ppb.currentCache, msg)
	if len(*ppb.currentCache) >= ppb.flushThreshold {
		ppb.bgFlush <- true
	}
}

// Flush runs in different goroutine with Put,
// listen flush event, when event is triggered,
// push msgs to hadnler and switch internal cache.
func (ppb *PingPongBuffer) Flush() {
	// flushTicker := time.NewTicker(ppb.flushInterval)

	for {
		select {
		case <-ppb.bgFlush:
			ppb.Lock()
			if len(*ppb.currentCache) > 0 {
				flushCache := ppb.currentCache
				if ppb.currentCache == &ppb.cache0 {
					ppb.currentCache = &ppb.cache1
				} else {
					ppb.currentCache = &ppb.cache0
				}
				ppb.Unlock()
				ppb.handler(*flushCache)
				*flushCache = nil
			} else {
				ppb.Unlock()
			}
		// case <-flushTicker.C:
		case <-ppb.exitChan:
			goto exit
		}
	}

exit:
	// flushTicker.Stop()
	ppb.cache0 = nil
	ppb.cache1 = nil
}
