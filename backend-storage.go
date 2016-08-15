package lmq

type BackendStorage interface {
	LoadTopicMeta(topic string) error
	OpenTopicForPersist(topic string)
	PersistMessages(topic string, msgs []*Message)
}
