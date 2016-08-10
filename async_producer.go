package lmq

import "time"

type ProducerMessage struct {
	Topic     string
	Body      []byte
	Timestamp time.Time
}

type AsyncProducer interface {
	AsyncClose()
	Close()
	Input() chan<- *ProducerMessage
}

type asyncProducer struct {
	queue Queue
	opt   *Options
	input chan *ProducerMessage
}

func NewAsyncProducer(queuePath string, opt *Options) (AsyncProducer, error) {
	return nil, nil
}
