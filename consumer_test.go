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
		topicName = "logTopic"
		topicOption = TopicOption{
			Name:                topicName,
			MaxBytesPerFile:     2 * 1024 * 1024,
			MaxDataFiles:        1000000,
			BufferSize:          1,
			BufferFlushInterval: 1 * time.Nanosecond,
			FetchSize:           100,
		}
		opt.Topics[topicName] = topicOption
	})

	JustBeforeEach(func() {
		aproducer, aperr = NewAsyncProducer(opt)
		consumer, cerr = NewConsumer("logConsumer", opt)
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

			Context("when publish data", func() {
				It("should be consumed correctly", func() {
					pm := &ProducerMessage{
						Topic:     "logTopic",
						Body:      []byte("hello lmq with cache full"),
						Timestamp: time.Now(),
					}
					for i := 0; i < 50000; i++ {
						aproducer.Input() <- pm
					}

					time.Sleep(1 * time.Second)

					aproducer.Close()

					tc, err := consumer.ConsumeTopic(topicName, 0)
					Expect(err).NotTo(HaveOccurred())
					msgs := tc.Messages()
					count := 0
					timeout := time.NewTimer(100 * time.Millisecond)
				Loop:
					for {
						select {
						case <-msgs:
							count++
							timeout.Reset(100 * time.Millisecond)
						case <-timeout.C:
							break Loop
						}
					}

					Expect(count).To(Equal(50000))
				})
			})
		})
	})
})
