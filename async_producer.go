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

	exitChan  chan int
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

func (p *asyncProducer) Close() {
	p.shutdown()
}

func (p *asyncProducer) Input() chan<- *ProducerMessage {
	return p.input
}

type topicProducer struct {
	parent *asyncProducer
	topic  string
	input  <-chan *ProducerMessage
}

func (p *asyncProducer) newTopicProducer(topic string) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.opt.Topics[topic].BufferSize)

	tp := &topicProducer{
		parent: p,
		topic:  topic,
		input:  input,
	}
	tp.loadTopicMeta()
	p.waitGroup.Wrap(func() { tp.putMessage() })

	return input
}

func (tp *topicProducer) loadTopicMeta() {
	err := tp.parent.queue.LoadTopicMeta(tp.topic)
	if err != nil {
		tp.parent.shutdown()
	}
}

func (p *asyncProducer) shutdown() {
	close(p.input)
	close(p.exitChan)
	p.waitGroup.Wait()
}

func (tp *topicProducer) putMessage() {
	for {
		select {
		case pMsg := <-tp.input:
			msg := NewMessage(pMsg.Body)
			tp.parent.queue.PutMessage(msg, tp.topic)
		case <-tp.parent.exitChan:
			goto exit
		}
	}
exit:
}
