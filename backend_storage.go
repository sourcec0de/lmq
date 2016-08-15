package lmq

type BackendStorage interface {
	OpenTopic(topic string, flag int) (Topic, error)
	PersistMessages(topic Topic, msgs []*Message)
}
