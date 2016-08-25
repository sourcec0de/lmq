package lmq_test

import (
	"reflect"
	"time"

	. "github.com/zwb-ict/lmq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("AsyncProducer", func() {
	var (
		aproducer AsyncProducer
		err       error
		opt       *Options
	)

	BeforeEach(func() {
		opt = &Options{
			DataPath:       "./queue_data",
			MaxTopicCount:  256,
			Topics:         make(map[string]TopicOption),
			BackendStorage: "Lmdb",
		}
		topicName := "log"
		topicOption := TopicOption{
			Name:                topicName,
			MaxBytesPerFile:     2 * 1024 * 1024,
			MaxDataFiles:        4,
			BufferSize:          100,
			BufferFlushInterval: 2 * time.Millisecond,
		}
		opt.Topics[topicName] = topicOption
	})

	JustBeforeEach(func() {
		aproducer, err = NewAsyncProducer(opt)
	})

	Context("when the async_producer creates succesfully", func() {
		It("should be an AsyncProducer object", func() {
			Expect(reflect.TypeOf(aproducer).String()).To(Equal("*lmq.asyncProducer"))
		})

		It("should not error", func() {
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
