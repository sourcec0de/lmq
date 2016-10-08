package lmq

import "time"

// ProducerMessage is the collection of elements passed to the Producer in order to send a message.
type ProducerMessage struct {
	Topic     string
	Body      []byte
	Timestamp time.Time
}

// AsyncProducer publishes messages using a non-blocking API.
// You must call Close() on a producer to avoid leaks:
// it will not be garbage-collected automatically when it
// passes out of scope.
type AsyncProducer interface {

	// Close shuts down the producer and flushes any messages it may have buffered.
	// You must call this function before a producer object passes out of scope, as
	// it may otherwise leak memory. You must call this before calling Close on the
	// underlying client.
	Close()

	// Input is the input channel for the user to write messages to that they
	// wish to send.
	Input() chan<- *ProducerMessage
}

type asyncProducer struct {
	queue Queue
	opt   *Options

	input chan *ProducerMessage

	tps []*topicProducer

	exitChan  chan struct{}
	waitGroup WaitGroupWrapper
}

// NewAsyncProducer creates a new AsyncProducer using the given option.
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

// NewAsyncProducerWithQueue creates a new Producer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this producer.
func NewAsyncProducerWithQueue(queue Queue) (AsyncProducer, error) {
	p := &asyncProducer{
		queue:    queue,
		opt:      queue.Option(),
		exitChan: make(chan struct{}),
		input:    make(chan *ProducerMessage),
	}

	p.waitGroup.Wrap(func() { p.dispatch() })

	return p, nil
}

func (p *asyncProducer) dispatch() {
	handlers := make(map[string]chan<- *ProducerMessage)

	for {
		pMsg, ok := <-p.input
		if !ok {
			goto exit
		}
		handler := handlers[pMsg.Topic]
		if handler == nil {
			handler = p.newTopicProducer(pMsg.Topic)
			handlers[pMsg.Topic] = handler
		}
		handler <- pMsg
	}

exit:
	for _, handler := range handlers {
		close(handler)
	}
}

func (p *asyncProducer) Close() {
	p.shutdown()
}

func (p *asyncProducer) Input() chan<- *ProducerMessage {
	return p.input
}

type topicProducer struct {
	parent *asyncProducer

	topic Topic

	input <-chan *ProducerMessage
	ppb   *PingPongBuffer
}

func (p *asyncProducer) newTopicProducer(topic string) chan<- *ProducerMessage {
	tp := &topicProducer{parent: p}
	p.tps = append(p.tps, tp)

	tp.topic = tp.openTopic(topic)

	topicOption := p.opt.Topics[topic]
	bufferSize := topicOption.BufferSize
	bufferFlushInterval := topicOption.BufferFlushInterval

	input := make(chan *ProducerMessage, bufferSize)
	tp.input = input
	tp.ppb = NewPingPongBuffer(
		p.exitChan,
		bufferSize,
		bufferFlushInterval,
		func(msgs []*Message) {
			p.queue.PutMessages(tp.topic, msgs)
		})

	p.waitGroup.Wrap(func() { tp.ppb.Flush() })
	p.waitGroup.Wrap(func() { tp.putMessage() })

	return input
}

func (tp *topicProducer) openTopic(topic string) Topic {
	return tp.parent.queue.OpenTopic(topic, "", 0)
}

func (p *asyncProducer) shutdown() {
	close(p.input)
	close(p.exitChan)
	p.waitGroup.Wait()
	p.closeTopics()
	p.queue.Close()
}

func (p *asyncProducer) closeTopics() {
	for _, tp := range p.tps {
		tp.closeTopic()
	}
}

func (tp *topicProducer) putMessage() {
	for {
		pMsg, ok := <-tp.input
		if !ok {
			goto exit
		}
		msg := NewMessage(pMsg.Body)
		tp.ppb.Put(msg)
	}
exit:
}

func (tp *topicProducer) closeTopic() {
	tp.parent.queue.CloseTopic(tp.topic)
}
