package lmq

type BackendStorage interface {
	LoadTopicMeta(topic string) error
	PersistMessages(topic string, msgs []*Message)
}
