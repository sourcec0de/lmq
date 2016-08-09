package lmq

type BackendStorage interface {
	InitTopicMeta(topic string)
	InitPartitionMeta(topic, partition string)
	PersistMessages(topic string, msgs []*Message)
}
