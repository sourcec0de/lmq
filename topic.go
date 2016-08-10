package lmq

type Topic interface {
	PutMessages(msgs []*Message)
}
