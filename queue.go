package lmq

import (
	"log"
	"sync"
)

// Queue manages topics
type Queue interface {
	Option() *Options
	OpenTopic(topic, groupID string, flag int) Topic
	PutMessages(topic Topic, msgs []*Message)
	ReadMessages(topic Topic, groupID string, msgs chan<- *[]byte)
	Stat(topic Topic) *TopicStat
	Close()
}

var (
	queueMap map[string]Queue
	l        sync.Mutex
)

func init() {
	queueMap = make(map[string]Queue)
}

type queue struct {
	opt *Options

	rTopics map[string]Topic
	wTopics map[string]Topic
	sync.Mutex

	backendStorage BackendStorage
}

// NewQueue creates a new Queue using the given option.
func NewQueue(opt *Options) (Queue, error) {
	l.Lock()
	defer l.Unlock()

	q, ok := queueMap[opt.DataPath]
	if ok {
		return q, nil
	}

	q = &queue{
		opt:     opt,
		rTopics: make(map[string]Topic),
		wTopics: make(map[string]Topic),
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

	queueMap[opt.DataPath] = q

	return q, nil
}

func (q *queue) Option() *Options {
	return q.opt
}

func (q *queue) OpenTopic(topic, groupID string, flag int) Topic {
	q.Lock()
	defer q.Unlock()

	switch flag {
	case 0:
		if t, ok := q.wTopics[topic]; ok {
			return t
		}
		t := q.backendStorage.OpenTopic(topic, groupID, flag)
		q.wTopics[topic] = t
		return t
	case 1:
		if t, ok := q.rTopics[topic]; ok {
			return t
		}
		t := q.backendStorage.OpenTopic(topic, groupID, flag)
		q.rTopics[topic] = t
		return t
	case 2:
		break
	default:
		log.Fatalf("Open topic faild: unvaild %d flag", flag)
	}
	return nil
}

func (q *queue) PutMessages(topic Topic, msgs []*Message) {
	q.backendStorage.PersistMessages(topic, msgs)
}

func (q *queue) ReadMessages(topic Topic, groupID string, msgs chan<- *[]byte) {
	q.backendStorage.ConsumeMessages(topic, groupID, msgs)
}

func (q *queue) Stat(topic Topic) *TopicStat {
	return q.backendStorage.Stat(topic)
}

func (q *queue) Close() {
	q.Lock()
	defer q.Unlock()

	for _, rt := range q.rTopics {
		q.backendStorage.CloseTopic(rt)
	}
	for _, wt := range q.wTopics {
		q.backendStorage.CloseTopic(wt)
	}

	q.backendStorage.Close()
}
