package lmq

type BoltBackendStorage struct {
}

func NewBoltBackendStorage(opt *Options) (BackendStorage, error) {
	return nil, nil
}

func (bbs *BoltBackendStorage) LoadTopicMeta(topic string) {

}

func (bbs *BoltBackendStorage) PersistMessages(topic string, msgs []*Message) {

}
