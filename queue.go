package lmq

import "sync"

type Queue interface {
	Open(opt *Options) error
	Option() *Options
	LoadTopicMeta(topic string) error
	OpenTopicForPersist(topic string)
	PutMessages(topic string, msgs []*Message)
	Close() error
}

var (
	queueManager struct {
		sync.RWMutex
		queueMap map[string]Queue
	}
)

type queue struct {
	opt            *Options
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
		opt: opt,
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
	return q.backendStorage.LoadTopicMeta(topic)
}

func (q *queue) OpenTopicForPersist(topic string) {
	q.backendStorage.OpenTopicForPersist(topic)
}

func (q *queue) PutMessages(topic string, msgs []*Message) {
	q.backendStorage.PersistMessages(topic, msgs)
}

func (q *queue) Close() error {
	return nil
}
