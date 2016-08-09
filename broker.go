package lmq

type Broker interface {
	OpenMQ(conf *Config) error
	OpenTopic(topic string) error
	PutMessage(msg *Message, topic string)
	Close() error
}
