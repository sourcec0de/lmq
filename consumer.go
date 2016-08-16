package lmq

type Consumer interface {
}

type consumer struct {
	opt      *Options
	queue    Queue
	name     string
	children map[string]*topicConsumer
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
		children: make(map[string]*topicConsumer),
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

}
