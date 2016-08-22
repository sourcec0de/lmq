package lmq

import "sync"

type Consumer interface {
}

type consumer struct {
	opt     *Options
	queue   Queue
	groupID string

	sync.Mutex
	children []*topicConsumer

	waitGroup WaitGroupWrapper
}

func NewConsumer(opt *Options) (Consumer, error) {
	queue, err := NewQueue(opt)
	if err != nil {
		return nil, err
	}
	c, err := NewConsumerWithQueue(queue)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func NewConsumerWithQueue(queue Queue) (Consumer, error) {
	c := &consumer{
		queue:    queue,
		opt:      queue.Option(),
		children: make([]*topicConsumer, 0),
	}
	return c, nil
}

type TopicConsumer interface{}
type topicConsumer struct {
	consumer *consumer
	opt      *Options

	topic    Topic
	messages chan *[]byte

	fetchSize int32
}

func (c *consumer) ConsumeTopic(topic string, offset uint64) (TopicConsumer, error) {
	t, err := c.openTopic(topic)
	if err != nil {
		return nil, err
	}

	child := &topicConsumer{
		consumer:  c,
		messages:  make(chan *[]byte, c.opt.Topics[topic].BufferSize),
		topic:     t,
		fetchSize: c.opt.Topics[topic].fetchSize,
	}

	c.addChild(child)

	c.waitGroup.Wrap(func() { child.readMessages() })

	return child, nil
}

func (c *consumer) openTopic(topic string) (Topic, error) {
	return c.queue.OpenTopic(topic, c.groupID, 1)
}

func (c *consumer) addChild(child *topicConsumer) {
	c.Lock()
	defer c.Unlock()

	c.children = append(c.children, child)
}

func (tc *topicConsumer) readMessages() {
	tc.consumer.queue.ReadMessages(tc.topic, tc.consumer.groupID, tc.messages)
}
