package lmq

// Queue manages topics
type Queue interface {
	Option() *Options
	OpenTopic(topic, groupID string, flag int) Topic
	PutMessages(topic Topic, msgs []*Message)
	ReadMessages(topic Topic, groupID string, msgs chan<- *[]byte)
	CloseTopic(topic Topic)
	Close()
}

type queue struct {
	opt            *Options
	backendStorage BackendStorage
}

// NewQueue creates a new Queue using the given option.
func NewQueue(opt *Options) (Queue, error) {
	q := &queue{
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

	q.backendStorage = backendStorage

	return q, nil
}

func (q *queue) Option() *Options {
	return q.opt
}

func (q *queue) OpenTopic(topic, groupID string, flag int) Topic {
	return q.backendStorage.OpenTopic(topic, groupID, flag)
}

func (q *queue) PutMessages(topic Topic, msgs []*Message) {
	q.backendStorage.PersistMessages(topic, msgs)
}

func (q *queue) ReadMessages(topic Topic, groupID string, msgs chan<- *[]byte) {
	q.backendStorage.ScanMessages(topic, groupID, msgs)
}

func (q *queue) CloseTopic(topic Topic) {
	q.backendStorage.CloseTopic(topic)
}

func (q *queue) Close() {
	q.backendStorage.Close()
}
