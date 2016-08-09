package lmq

import "time"

type PingPongBuffer struct {
	cache0       []*Message
	cache1       []*Message
	currentCache *[]*Message

	flushThreshold int
	bgFlush        chan bool
	flushInterval  time.Duration

	handler func(msgs []*Message)
}

func NewPingPongBuffer(size int, flushInterval time.Duration, handler func(msgs []*Message)) *PingPongBuffer {
	ppb := &PingPongBuffer{
		cache0:         make([]*Message, size),
		cache1:         make([]*Message, size),
		flushThreshold: size,
		flushInterval:  flushInterval,
		handler:        handler,
	}
	ppb.currentCache = &ppb.cache0
	return ppb
}

func (ppb *PingPongBuffer) Put(msg *Message) {

}
