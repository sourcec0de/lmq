package lmq

type LmdbBackendStorage struct {
}

func NewLmdbBackendStorage(opt *Options) (BackendStorage, error) {

	return nil, nil
}

func (lbs *LmdbBackendStorage) InitTopicMeta(topic string) {

}

func (lbs *LmdbBackendStorage) InitPartitionMeta(topic, partition string) {

}

func (lbs *LmdbBackendStorage) PersistMessages(topic string, msgs []*Message) {

}
