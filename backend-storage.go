package lmq

type BackendStorage interface {
	OpenTopic(topic string) (Topic, error)
	PersistMessages(topic string, msgs []*Message)
}
