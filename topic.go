package lmq

type Topic interface {
	PutMessages(msgs []*Message)
	LoadTopicMeta() error
}

type topic struct {
	name  string
	queue Queue
}

func NewTopic(name string, queue Queue) Topic {
	t := &topic{
		name:  name,
		queue: queue,
	}
	return t
}

func (t *topic) LoadTopicMeta() error {
	bs := t.queue.BackendStorage()
	return bs.LoadTopicMeta(t.name)
}

func (t *topic) PutMessages(msgs []*Message) {

}
