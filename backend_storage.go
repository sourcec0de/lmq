package lmq

type BackendStorage interface {
	OpenTopic(topic, groupID string, flag int) Topic
	PersistMessages(topic Topic, msgs []*Message)
	ScanMessages(topic Topic, groupID string, msgs chan<- *[]byte)
	Stat(topic Topic) *TopicStat
	CloseTopic(topic Topic)
	Close()
}
