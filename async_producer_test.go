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
		aproducer   AsyncProducer
		err         error
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
		aproducer, err = NewAsyncProducer(opt)
	})

	Context("when the aproducer creates succesfully", func() {
		It("should be an AsyncProducer object", func() {
			Expect(reflect.TypeOf(aproducer).String()).To(Equal("*lmq.asyncProducer"))
		})

		It("should not error", func() {
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when publish data to 'log' topic", func() {
			Context("always publish because full cache", func() {
				It("should publish succesfully (full cache)", func() {
					pm := &ProducerMessage{
						Topic:     "log",
						Body:      []byte("hello lmq with cache full"),
						Timestamp: time.Now(),
					}
					for i := 0; i < 100000; i++ {
						aproducer.Input() <- pm
					}
					aproducer.Close()
				})
			})
		})
	})

	Context("when fails to create aproducer", func() {
		BeforeEach(func() {
			opt.DataPath = "./invalid_data_path"
		})

		It("should return nil", func() {
			Expect(aproducer).To(BeNil())
		})

		It("should error", func() {
			Expect(err).To(HaveOccurred())
		})
	})
})
