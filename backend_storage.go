package lmq

type BackendStorage interface {
	OpenTopic(topic string) (Topic, error)
	PersistMessages(topic Topic, msgs []*Message)
}
