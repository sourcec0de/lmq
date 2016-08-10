package lmq

type BoltBackendStorage struct {
}

func NewBoltBackendStorage(path string, conf *Config) (BackendStorage, error) {
	return nil, nil
}

func (lbs *BoltBackendStorage) InitTopicMeta(topic string) {

}

func (lbs *BoltBackendStorage) InitPartitionMeta(topic, partition string) {

}

func (lbs *BoltBackendStorage) PersistMessages(topic string, msgs []*Message) {

}
