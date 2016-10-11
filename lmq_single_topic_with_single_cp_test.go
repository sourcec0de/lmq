package lmq_test

import (
	"reflect"
	"time"

	. "github.com/zwb-ict/lmq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LmqSingleTopicWithSingleCp", func() {
	var (
		aproducer AsyncProducer
		aperr     error

		consumer Consumer
		cerr     error

		topicName string

		opt         *Options
		topicOption TopicOption

		queue Queue
		qerr  error

		msgsTotalCount int
		msg            string
	)

	BeforeEach(func() {
		opt = &Options{
			DataPath:       "./queue_data_single_topic_with_single_cp",
			MaxTopicCount:  1,
			Topics:         make(map[string]TopicOption),
			BackendStorage: "Lmdb",
		}
		topicName = "single"
		topicOption = TopicOption{
			Name:                topicName,
			MaxBytesPerFile:     2 * 1024 * 1024,
			MaxDataFiles:        50,
			BufferSize:          1,
			BufferFlushInterval: 2 * time.Millisecond,
			FetchSize:           100,
		}
		opt.Topics[topicName] = topicOption

		queue, qerr = NewQueue(opt)

		msgsTotalCount = 8000
		msg = "hello lmq with single topic with single cp"
	})

	JustBeforeEach(func() {
		aproducer, aperr = NewAsyncProducer(queue)
		consumer, cerr = NewConsumer("single", queue)
	})

	Context("when the queue create successfully", func() {
		It("queue should be a Queue object", func() {
			Expect(reflect.TypeOf(queue).String()).To(Equal("*lmq.queue"))
		})
		It("qerr should be nil", func() {
			Expect(qerr).NotTo(HaveOccurred())
		})
	})

	Context("when the aproducer, consumer create succesfully", func() {
		It("aproducer should be an AsyncProducer object", func() {
			Expect(reflect.TypeOf(aproducer).String()).To(Equal("*lmq.asyncProducer"))
		})

		It("aperr should be nil", func() {
			Expect(aperr).NotTo(HaveOccurred())
		})

		It("consumer should be an Consumer object", func() {
			Expect(reflect.TypeOf(consumer).String()).To(Equal("*lmq.consumer"))
		})

		It("cerr should be nil", func() {
			Expect(cerr).NotTo(HaveOccurred())
		})

		Context("and publish and consume msgs", func() {
			It("consumed count should be equal with produced count", func() {
				for i := 0; i < msgsTotalCount; i++ {
					aproducer.Input() <- &ProducerMessage{
						Topic:     topicName,
						Body:      []byte(msg),
						Timestamp: time.Now(),
					}
				}

				time.Sleep(5 * time.Second)

				tc, err := consumer.ConsumeTopic(topicName, 0)
				Expect(err).NotTo(HaveOccurred())

				msgs := tc.Messages()
				consumedCount := 0
				timeout := time.NewTimer(100 * time.Millisecond)
				for {
					select {
					case <-msgs:
						consumedCount++
						timeout.Reset(2 * time.Second)
					case <-timeout.C:
						goto result
					}
				}
			result:
				Expect(consumedCount).To(Equal(msgsTotalCount))

				// queue.Close()
			})
		})
	})
})
