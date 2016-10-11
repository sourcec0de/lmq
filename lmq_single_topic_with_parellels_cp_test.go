package lmq_test

import (
	"reflect"
	"time"

	. "github.com/zwb-ict/lmq"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LmqSingleTopicWithParellelsCp", func() {
	var (
		parallels int

		aproducers []AsyncProducer
		aperrs     []error

		consumers []Consumer
		cerrs     []error

		topicName string

		opt         *Options
		topicOption TopicOption

		queue Queue
		qerr  error

		msgsTotalCount int
		msg            string

		waitGroup WaitGroupWrapper
	)

	BeforeEach(func() {
		parallels = 4

		aproducers = make([]AsyncProducer, parallels)
		aperrs = make([]error, parallels)

		consumers = make([]Consumer, parallels)
		cerrs = make([]error, parallels)

		opt = &Options{
			DataPath:       "./queue_data_single_topic_with_parallels_cp",
			MaxTopicCount:  1,
			Topics:         make(map[string]TopicOption),
			BackendStorage: "Lmdb",
		}
		topicName = "parallels"
		topicOption = TopicOption{
			Name:                topicName,
			MaxBytesPerFile:     2 * 1024 * 1024,
			MaxDataFiles:        50,
			BufferSize:          100,
			BufferFlushInterval: 2 * time.Millisecond,
			FetchSize:           100,
		}
		opt.Topics[topicName] = topicOption

		queue, qerr = NewQueue(opt)

		msgsTotalCount = 2000
		msg = "hello lmq with single topic with single cp"

	})

	JustBeforeEach(func() {
		for i := 0; i < parallels; i++ {
			aproducers[i], aperrs[i] = NewAsyncProducer(queue)
			consumers[i], cerrs[i] = NewConsumer("single", queue)
		}
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
		It("aproducers should be all AsyncProducer objects", func() {
			for i := 0; i < parallels; i++ {
				Expect(reflect.TypeOf(aproducers[i]).String()).To(Equal("*lmq.asyncProducer"))
			}
		})

		It("aperrs should be all nil", func() {
			for i := 0; i < parallels; i++ {
				Expect(aperrs[i]).NotTo(HaveOccurred())
			}
		})

		It("consumer should be all Consumer objects", func() {
			for i := 0; i < parallels; i++ {
				Expect(reflect.TypeOf(consumers[i]).String()).To(Equal("*lmq.consumer"))
			}
		})

		It("cerrs should be all nil", func() {
			for i := 0; i < parallels; i++ {
				Expect(cerrs[i]).NotTo(HaveOccurred())
			}
		})

		Context("then publish and consume msgs", func() {
			It("consumed count should be equal with produced count", func(done Done) {
				totalConsumedCounts := 0
				consumedCounts := make(chan int, parallels)
				for i := 0; i < parallels; i++ {
					waitGroup.Wrap(func(i int) func() {
						return func() {
							for j := 0; j < msgsTotalCount; j++ {
								aproducers[i].Input() <- &ProducerMessage{
									Topic:     topicName,
									Body:      []byte(msg),
									Timestamp: time.Now(),
								}
							}
						}
					}(i))

					time.Sleep(5 * time.Second)

					waitGroup.Wrap(func(i int) func() {
						return func() {
							tc, err := consumers[i].ConsumeTopic(topicName, 0)
							Expect(err).NotTo(HaveOccurred())
							msgs := tc.Messages()
							timeout := time.NewTimer(100 * time.Millisecond)
							consumedCount := 0
						loop:
							for {
								select {
								case <-msgs:
									consumedCount++
									timeout.Reset(10 * time.Second)
								case <-timeout.C:
									consumedCounts <- consumedCount
									break loop
								}
							}
						}
					}(i))
				}

				waitGroup.Wait()

				timeout := time.NewTimer(100 * time.Millisecond)
				for {
					select {
					case consumedCount := <-consumedCounts:
						totalConsumedCounts += consumedCount
						timeout.Reset(100 * time.Millisecond)
					case <-timeout.C:
						goto result
					}
				}
			result:
				Expect(totalConsumedCounts).To(Equal(msgsTotalCount * parallels))

				// queue.Close()

				close(done)
			}, 10000)
		})
	})
})
