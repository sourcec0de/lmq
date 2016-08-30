package lmq

import "time"

type TopicOption struct {
	Name string

	MaxBytesPerFile int64
	MaxDataFiles    uint64

	BufferSize          int
	BufferFlushInterval time.Duration

	FetchSize int32
}

type Options struct {
	DataPath      string
	MaxTopicCount int

	Topics map[string]TopicOption

	BackendStorage string
}
