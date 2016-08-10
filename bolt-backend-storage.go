package lmq

type BoltBackendStorage struct {
}

func NewBoltBackendStorage(path string, conf *Config) (BackendStorage, error) {
	return nil, nil
}

func (bbs *BoltBackendStorage) InitTopicMeta(topic string) {

}

func (bbs *BoltBackendStorage) InitPartitionMeta(topic, partition string) {

}

func (bbs *BoltBackendStorage) PersistMessages(topic string, msgs []*Message) {

}
