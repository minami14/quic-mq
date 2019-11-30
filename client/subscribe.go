package client

import (
	"context"

	"github.com/lucas-clemente/quic-go"
)

// SubConn is a subscription stream.
type SubConn struct {
	receive chan []byte
	topic   string
	stream  quic.ReceiveStream
	conn    *Conn
	ctx     context.Context
}

func newSubConn(topic string, stream quic.ReceiveStream, conn *Conn) *SubConn {
	return &SubConn{
		receive: make(chan []byte),
		topic:   topic,
		stream:  stream,
		conn:    conn,
	}
}

func (c *SubConn) start(ctx context.Context) error {
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
func (c *SubConn) Topic() string {
	return c.topic
}

// Receive returns received message.
func (c *SubConn) Receive() <-chan []byte {
	return c.receive
}

// Done channel for cancellation.
func (c *SubConn) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Close closes the stream.
func (c *SubConn) Close() error {
	return c.conn.CancelSubscribe(c.topic)
}
