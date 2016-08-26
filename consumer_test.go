package lmq_test

import (
	"reflect"
	"time"

	. "github.com/zwb-ict/lmq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Consumer", func() {
	var (
		aproducer   AsyncProducer
		consumer    Consumer
		aperr       error
		cerr        error
		topicName   string
		opt         *Options
		topicOption TopicOption
	)

	BeforeEach(func() {
		opt = &Options{
			DataPath:       "./queue_data",
			MaxTopicCount:  256,
			Topics:         make(map[string]TopicOption),
			BackendStorage: "Lmdb",
		}
		topicName = "log"
		topicOption = TopicOption{
			Name:                topicName,
			MaxBytesPerFile:     2 * 1024 * 1024,
			MaxDataFiles:        4,
			BufferSize:          40,
			BufferFlushInterval: 2 * time.Millisecond,
		}
		opt.Topics[topicName] = topicOption
	})

	JustBeforeEach(func() {
		aproducer, aperr = NewAsyncProducer(opt)
		consumer, cerr = NewConsumer(opt)
	})

	Context("when the aproducer creates succesfully", func() {
		It("should be an AsyncProducer object", func() {
			Expect(reflect.TypeOf(aproducer).String()).To(Equal("*lmq.asyncProducer"))
		})

		It("should not error", func() {
			Expect(aperr).NotTo(HaveOccurred())
		})

		Context("when the consumer creates succesfully", func() {
			It("should be an consumer object", func() {
				Expect(reflect.TypeOf(consumer).String()).To(Equal("*lmq.consumer"))
			})

			It("should not error", func() {
				Expect(cerr).NotTo(HaveOccurred())
			})
		})
	})
})
