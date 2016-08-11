package lmq

import "sync"

type Queue interface {
	Open(opt *Options) error
	Option() *Options
	BackendStorage() BackendStorage
	LoadTopicMeta(topic string) error
	PutMessage(msg *Message, topic string)
	Close() error
}

var (
	queueManager struct {
		sync.RWMutex
		queueMap map[string]Queue
	}
)

type queue struct {
	opt *Options

	sync.RWMutex
	topicMap map[string]Topic

	backendStorage BackendStorage
}

func NewQueue(opt *Options) (Queue, error) {
	queueManager.RLock()
	q, ok := queueManager.queueMap[opt.DataPath]
	queueManager.RUnlock()
	if ok {
		return q, nil
	}

	queueManager.Lock()

	q, ok = queueManager.queueMap[opt.DataPath]
	if ok {
		queueManager.Unlock()
		return q, nil
	}

	q = &queue{
		opt:      opt,
		topicMap: make(map[string]Topic),
	}

	var backendStorage BackendStorage
	var err error
	switch opt.BackendStorage {
	case "Bolt":
		backendStorage, err = NewBoltBackendStorage(opt)
	case "Lmdb":
		backendStorage, err = NewLmdbBackendStorage(opt)
	}
	if err != nil {
		return nil, err
	}
	q.(*queue).backendStorage = backendStorage
	queueManager.queueMap[opt.DataPath] = q
	queueManager.Unlock()

	return q, nil
}

func (q *queue) Open(opt *Options) error {
	return nil
}

func (q *queue) Option() *Options {
	return q.opt
}

func (q *queue) LoadTopicMeta(topic string) error {
	q.RLock()
	_, ok := q.topicMap[topic]
	q.Unlock()
	if ok {
		return nil
	}

	q.Lock()

	_, ok = q.topicMap[topic]
	if ok {
		return nil
	}

	t := NewTopic(topic, q)
	err := t.LoadTopicMeta()
	if err != nil {
		return err
	}
	q.topicMap[topic] = t
	q.Unlock()
	return nil
}

func (q *queue) PutMessage(msg *Message, topic string) {

}

func (q *queue) Close() error {
	return nil
}

func (q *queue) BackendStorage() BackendStorage {
	return q.backendStorage
}
