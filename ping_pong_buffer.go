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

}

func (ppb *PingPongBuffer) Put(msg *Message) {

}
