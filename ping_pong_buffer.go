package lmq

import (
	"sync"
	"time"
)

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

func NewPingPongBuffer(exitChan <-chan int, size int, flushInterval time.Duration, handler func(msgs []*Message)) *PingPongBuffer {
	ppb := &PingPongBuffer{
		cache0:         make([]*Message, size),
		cache1:         make([]*Message, size),
		flushThreshold: size,
		bgFlush:        make(chan bool),
		flushInterval:  flushInterval,
		handler:        handler,
	}
	ppb.currentCache = &ppb.cache0

	return ppb
}

func (ppb *PingPongBuffer) Put(msg *Message) {
	ppb.Lock()
	defer ppb.Unlock()

	cache := *ppb.currentCache
	cache = append(cache, msg)
	if len(cache) >= ppb.flushThreshold {
		ppb.bgFlush <- true
	}
}

func (ppb *PingPongBuffer) Flush() {
	flushTicker := time.NewTicker(ppb.flushInterval)

	for {
		select {
		case <-ppb.bgFlush:
		case <-flushTicker.C:
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
		case <-ppb.exitChan:
			goto exit
		}
	}

exit:
	flushTicker.Stop()
	ppb.cache0 = nil
	ppb.cache1 = nil
}
