package lmq

type Topic interface {
	OpenPartitionForPersist()
	PersistMessages(msgs []*Message)
	OpenPartitionForConsume(groupID string)
	ConsumeMessages(groupID string, msgs chan<- *[]byte)
}
