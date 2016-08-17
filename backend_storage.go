package lmq

type BackendStorage interface {
	OpenTopic(topic, groupID string, flag int) (Topic, error)
	PersistMessages(topic Topic, msgs []*Message)
	ScanMessages(topic Topic, msgs chan<- *[]byte)
}
