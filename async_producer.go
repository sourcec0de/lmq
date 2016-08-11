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

	exitChan  chan int
	waitGroup WaitGroupWrapper
}

func NewAsyncProducer(opt *Options) (AsyncProducer, error) {
	queue, err := NewQueue(opt)
	if err != nil {
		return nil, err
	}

	p, err := NewAsyncProducerWithQueue(queue)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func NewAsyncProducerWithQueue(queue Queue) (AsyncProducer, error) {
	p := &asyncProducer{
		queue:    queue,
		opt:      queue.Option(),
		exitChan: make(chan int),
		input:    make(chan *ProducerMessage),
	}

	p.waitGroup.Wrap(func() { p.dispatch() })

	return p, nil
}

func (p *asyncProducer) dispatch() {
	handlers := make(map[string]chan<- *ProducerMessage)

	for {
		select {
		case pMsg := <-p.input:
			handler := handlers[pMsg.Topic]
			if handler == nil {
				handler = p.newTopicProducer(pMsg.Topic)
				handlers[pMsg.Topic] = handler
			}
			handler <- pMsg
		case <-p.exitChan:
			goto exit
		}
	}

exit:
	for _, handler := range handlers {
		close(handler)
	}
}

func (p *asyncProducer) AsyncClose() {

}

func (p *asyncProducer) Close() {

}

func (p *asyncProducer) Input() chan<- *ProducerMessage {
	return p.input
}

type topicProducer struct {
	parent *asyncProducer
	topic  string
	input  <-chan *ProducerMessage
}
