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

	exitChan <-chan struct{}

	handler func(msgs []*Message)
}

// NewPingPongBuffer creates a new PingPongBuffer using the given option.
// exitChan: when close, notify flush goroutine to exit.
// flushThreshold: flush goroutine will run when internal buffer size >= this value.
// flushInterval: flush goroutine will run when tick timeout.
// handler: when cache full, flush goroutine will push data to handler.
func NewPingPongBuffer(exitChan <-chan struct{}, flushThreshold int, flushInterval time.Duration, handler func(msgs []*Message)) *PingPongBuffer {
	ppb := &PingPongBuffer{
		cache0:         make([]*Message, 0),
		cache1:         make([]*Message, 0),
		flushThreshold: flushThreshold,
		bgFlush:        make(chan bool),
		flushInterval:  flushInterval,
		exitChan:       exitChan,
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

	*ppb.currentCache = append(*ppb.currentCache, msg)
	if len(*ppb.currentCache) >= ppb.flushThreshold {
		ppb.Unlock()
		ppb.bgFlush <- true
		return
	}

	ppb.Unlock()
}

// Flush runs in different goroutine with Put,
// listen flush event, when event is triggered,
// push msgs to hadnler and switch internal cache.
func (ppb *PingPongBuffer) Flush() {
	flushTicker := time.NewTicker(ppb.flushInterval)

	for {
		select {
		case <-ppb.bgFlush:
			ppb.swithAndFlush()
		case <-flushTicker.C:
			ppb.swithAndFlush()
		case <-ppb.exitChan:
			goto exit
		}
	}

exit:
	flushTicker.Stop()
	ppb.cache0 = nil
	ppb.cache1 = nil
}

func (ppb *PingPongBuffer) swithAndFlush() {
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
}
