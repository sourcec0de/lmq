package lmq

type BackendStorage interface {
	LoadTopicMeta(topic string) error
	InitPartitionMeta(topic, partition string)
	PersistMessages(topic string, msgs []*Message)
}
