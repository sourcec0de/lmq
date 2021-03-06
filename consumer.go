package lmq

import (
	"sync"
	"time"
)

type Consumer interface {
	ConsumeTopic(topic string, offset uint64) (TopicConsumer, error)
	Close()
}

type consumer struct {
	opt     *Options
	queue   Queue
	groupID string

	sync.Mutex
	children []*topicConsumer

	exitChan  chan struct{}
	waitGroup WaitGroupWrapper
}

func NewConsumer(groupID string, queue Queue) (Consumer, error) {
	c := &consumer{
		queue:    queue,
		opt:      queue.Option(),
		groupID:  groupID,
		children: make([]*topicConsumer, 0),
		exitChan: make(chan struct{}),
	}
	return c, nil
}

type TopicConsumer interface {
	Messages() <-chan *[]byte
	Close()
}

type topicConsumer struct {
	consumer *consumer
	opt      *Options

	topic    Topic
	messages chan *[]byte

	fetchSize int32

	exitChan chan struct{}
}

func (c *consumer) ConsumeTopic(topic string, offset uint64) (TopicConsumer, error) {
	child := &topicConsumer{
		consumer:  c,
		messages:  make(chan *[]byte, c.opt.Topics[topic].BufferSize),
		fetchSize: c.opt.Topics[topic].FetchSize,
		exitChan:  make(chan struct{}),
	}

	c.addChild(child)

	c.waitGroup.Wrap(func() {
		child.topic = c.openTopic(topic)
		child.readMessages()
	})

	return child, nil
}

func (c *consumer) Close() {
	close(c.exitChan)
	c.waitGroup.Wait()
}

func (c *consumer) openTopic(topic string) Topic {
	return c.queue.OpenTopic(topic, c.groupID, 1)
}

func (c *consumer) addChild(child *topicConsumer) {
	c.Lock()
	defer c.Unlock()

	c.children = append(c.children, child)
}

func (tc *topicConsumer) readMessages() {
	for {
		select {
		case <-tc.consumer.exitChan:
			goto exit
		case <-tc.exitChan:
			goto exit
		default:
			tc.consumer.queue.ReadMessages(tc.topic, tc.consumer.groupID, tc.messages)
			time.Sleep(50 * time.Millisecond)
		}
	}
exit:
	close(tc.messages)
}

func (tc *topicConsumer) Messages() <-chan *[]byte {
	return tc.messages
}

func (tc *topicConsumer) Close() {
	close(tc.exitChan)
}
