package lmq

type Queue interface {
	Open(conf *Config) error
	LoadTopicMeta(topic string) error
	PutMessage(msg *Message, topic string)
	Close() error
}
