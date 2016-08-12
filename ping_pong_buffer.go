package lmq

import "time"

type PingPongBuffer struct {
	cache0       []*Message
	cache1       []*Message
	currentCache *[]*Message

	flushThreshold uint64
	bgFlush        chan bool
	flushInterval  time.Duration

	handler func(topic string, msgs []*Message)
}

func NewPingPongBuffer(size uint64, flushInterval time.Duration, handler func(topic string, msgs []*Message)) *PingPongBuffer {
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

}
