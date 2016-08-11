package lmq

type Message struct {
	Body []byte
}

func NewMessage(body []byte) *Message {
	return &Message{
		Body: body,
	}
}
