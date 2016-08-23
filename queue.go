package lmq

import "sync"

// Queue manages topics
type Queue interface {
	Option() *Options
	OpenTopic(topic, groupID string, flag int) (Topic, error)
	PutMessages(topic Topic, msgs []*Message)
	ReadMessages(topic Topic, groupID string, msgs chan<- *[]byte)
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

// NewQueue creates a new Queue using the given option.
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

func (q *queue) Option() *Options {
	return q.opt
}

func (q *queue) OpenTopic(topic, groupID string, flag int) (Topic, error) {
	return q.backendStorage.OpenTopic(topic, groupID, flag)
}

func (q *queue) PutMessages(topic Topic, msgs []*Message) {
	q.backendStorage.PersistMessages(topic, msgs)
}

func (q *queue) ReadMessages(topic Topic, groupID string, msgs chan<- *[]byte) {
	q.backendStorage.ScanMessages(topic, groupID, msgs)
}
