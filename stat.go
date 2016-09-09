package lmq

func Stat(opt *Options, topic string) *TopicStat {
	q, err := NewQueue(opt)
	if err != nil {
		return nil
	}
	return q.Stat(q.OpenTopic(topic, "", 2))
}
