package lmq

import "time"

type TopicOption struct {
	Name string

	MaxBytesPerFile string
	MaxDataFiles    uint64

	BufferSize          int
	BufferFlushInterval time.Duration
}

type Options struct {
	DataPath      string
	MaxTopicCount int

	Topics map[string]TopicOption

	BackendStorage string
}
