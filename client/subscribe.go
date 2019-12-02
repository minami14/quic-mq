package client

import (
	"context"

	"github.com/lucas-clemente/quic-go"
)

// SubscribeStream is a subscription stream.
type SubscribeStream struct {
	receive chan []byte
	topic   string
	stream  quic.ReceiveStream
	conn    *Conn
	ctx     context.Context
}

func newSubscribeStream(topic string, stream quic.ReceiveStream, conn *Conn) *SubscribeStream {
	return &SubscribeStream{
		receive: make(chan []byte),
		topic:   topic,
		stream:  stream,
		conn:    conn,
	}
}

func (c *SubscribeStream) start(ctx context.Context) error {
	c.ctx = ctx
	buf := make([]byte, c.conn.config.MaxMessageSize)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		n, err := c.conn.read(buf, c.stream)
		if err != nil {
			return err
		}

		if n == 0 {
			return nil
		}

		c.receive <- buf[:n]
	}
}

// Topic is a subscribed topic.
func (c *SubscribeStream) Topic() string {
	return c.topic
}

// Receive returns received message.
func (c *SubscribeStream) Receive() <-chan []byte {
	return c.receive
}

// Done channel for cancellation.
func (c *SubscribeStream) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Close closes the stream.
func (c *SubscribeStream) Close() error {
	return c.conn.unsubscribe(c.topic)
}
