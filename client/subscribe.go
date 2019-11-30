package client

import (
	"context"
	"github.com/lucas-clemente/quic-go"
)

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

func (c *SubConn) Topic() string {
	return c.topic
}

func (c *SubConn) Receive() <-chan []byte {
	return c.receive
}

func (c *SubConn) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *SubConn) Close() error {
	return c.conn.CancelSubscribe(c.topic)
}
