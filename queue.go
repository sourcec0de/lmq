package lmq

import "sync"

type Queue interface {
	Open(opt *Options) error
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

func NewQueue(path string, opt *Options) (Queue, error) {
	queueManager.RLock()
	q, ok := queueManager.queueMap[path]
	queueManager.RUnlock()
	if ok {
		return q, nil
	}

	queueManager.Lock()

	q, ok = queueManager.queueMap[path]
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
		backendStorage, err = NewBoltBackendStorage(path, opt)
	case "Lmdb":
		backendStorage, err = NewLmdbBackendStorage(path, opt)
	}
	if err != nil {
		return nil, err
	}
	q.(*queue).backendStorage = backendStorage
	queueManager.queueMap[path] = q
	queueManager.Unlock()

	return q, nil
}

func (q *queue) Open(opt *Options) error {
	return nil
}

func (q *queue) LoadTopicMeta(topic string) error {
	return nil
}

func (q *queue) PutMessage(msg *Message, topic string) {

}

func (q *queue) Close() error {
	return nil
}
