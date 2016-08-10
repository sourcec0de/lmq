package lmq

import "time"

type TopicOption struct {
	Name string

	MaxBytesPerFile string
	MaxDataFiles    uint64

	BufferSize          uint64
	BufferFlushInterval time.Duration
}

type Options struct {
	DataPath string

	Topics []TopicOption

	BackendStorage string
}
