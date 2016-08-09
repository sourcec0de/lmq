package lmq

import "time"

type PingPongBuffser struct {
	cache0       []*Message
	cache1       []*Message
	currentCache *[]*Message

	flushThreshold int
	bgFlush        chan bool
	flushInterval  time.Duration

	handler func(msgs []*Message)
}

func NewPingPongBuffer(size int, flushInterval time.Duration, handler func(msgs []*Message)) *PingPongBuffser {

}

func (ppb *PingPongBuffser) Put(msg *Message) {

}
