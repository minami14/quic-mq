package client

import (
	"context"

	"github.com/lucas-clemente/quic-go"
)

// SubStream is a subscription stream.
type SubStream struct {
	receive chan []byte
	topic   string
	stream  quic.ReceiveStream
	conn    *Conn
	ctx     context.Context
}

func newSubConn(topic string, stream quic.ReceiveStream, conn *Conn) *SubStream {
	return &SubStream{
		receive: make(chan []byte),
		topic:   topic,
		stream:  stream,
		conn:    conn,
	}
}

func (c *SubStream) start(ctx context.Context) error {
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
func (c *SubStream) Topic() string {
	return c.topic
}

// Receive returns received message.
func (c *SubStream) Receive() <-chan []byte {
	return c.receive
}

// Done channel for cancellation.
func (c *SubStream) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Close closes the stream.
func (c *SubStream) Close() error {
	return c.conn.CancelSubscribe(c.topic)
}
