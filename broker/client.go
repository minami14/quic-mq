package broker

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/lucas-clemente/quic-go"
)

type client struct {
	broker     *MessageBroker
	stream     quic.Stream
	subStreams map[string]quic.SendStream
	pubStreams map[string]context.CancelFunc
	session    quic.Session
}

func newClient(broker *MessageBroker, session quic.Session) *client {
	return &client{
		broker:     broker,
		subStreams: map[string]quic.SendStream{},
		pubStreams: map[string]context.CancelFunc{},
		session:    session,
	}
}

func (c *client) run(ctx context.Context) error {
	stream, err := c.session.AcceptStream(ctx)
	defer func() {
		_ = stream.Close()
	}()

	if err != nil {
		return err
	}

	c.stream = stream
	buf := make([]byte, c.broker.maxMessageByte)
	if err := c.verify(buf); err != nil {
		binary.LittleEndian.PutUint16(buf, 1)
		buf[2] = authErr
		_, _ = stream.Write(buf[:3])
		return err
	}

	binary.LittleEndian.PutUint16(buf, 1)
	buf[2] = statusOK
	if _, err := stream.Write(buf[:3]); err != nil {
		return err
	}

	return c.startStream(ctx, buf)
}

func (c *client) verify(buf []byte) error {
	n, err := c.read(buf, c.stream)
	if err != nil {
		return err
	}

	uid := string(buf[:n])
	n, err = c.read(buf, c.stream)
	if err != nil {
		return err
	}

	password := string(buf[:n])
	if !c.broker.userManger.verify(uid, password) {
		return fmt.Errorf("authentication failure %v %v", uid, password)
	}

	return nil
}

const (
	startPublish = iota + 1
	cancelPublish
	subscribe
	unsubscribe
)

const (
	statusOK = iota + 1
	authErr
)

const (
	pub = iota + 1
	sub
)

func (c *client) startStream(ctx context.Context, buf []byte) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		n, err := c.read(buf, c.stream)
		if err != nil {
			c.closeStreams()
			return err
		}

		if n < 2 {
			c.closeStreams()
			return nil
		}

		messageType := buf[0]
		topic := string(buf[1:n])

		switch messageType {
		case startPublish:
			if err := c.startPublish(ctx, topic); err != nil {
				return err
			}
		case cancelPublish:
			c.cancelPublish(topic)
		case subscribe:
			if err := c.subscribe(ctx, topic, buf); err != nil {
				return err
			}
		case unsubscribe:
			c.unsubscribe(topic)
		default:
			return fmt.Errorf("invalid message %v", buf)
		}
	}
}

const (
	buffered = iota + 1
	// unbuffered
)

func (c *client) startPublish(ctx context.Context, topic string) error {
	stream, err := c.session.OpenStreamSync(ctx)
	if err != nil {
		return err
	}

	buf := make([]byte, c.broker.maxMessageByte)
	binary.LittleEndian.PutUint16(buf, uint16(len(topic)+1))
	buf[2] = pub
	copy(buf[3:], topic)
	if _, err := stream.Write(buf[:len(topic)+3]); err != nil {
		return err
	}

	go func() {
		defer func() {
			_ = stream.Close()
		}()

		ctx, cancel := context.WithCancel(ctx)
		c.pubStreams[topic] = cancel
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			n, err := c.read(buf, stream)
			if err != nil {
				c.broker.logger.Println(err)
				return
			}

			if buf[0] == buffered {
				c.broker.bufferManager.store(topic, buf[1:n])
			}

			c.broker.subscriptionManager.publish(topic, buf[1:n])
		}
	}()

	return nil
}

func (c *client) cancelPublish(topic string) {
	cancel, ok := c.pubStreams[topic]
	if ok {
		cancel()
		delete(c.pubStreams, topic)
	}
}

func (c *client) subscribe(ctx context.Context, topic string, buf []byte) error {
	if _, err := c.read(buf, c.stream); err != nil {
		return err
	}

	count := int(binary.LittleEndian.Uint16(buf))
	binary.LittleEndian.PutUint16(buf, uint16(len(topic)+1))
	n := 2
	buf[n] = sub
	n++
	copy(buf[n:n+len(topic)], topic)
	n += len(topic)
	stream, err := c.session.OpenUniStreamSync(ctx)
	if err != nil {
		return err
	}

	if _, err := stream.Write(buf[:n]); err != nil {
		return err
	}

	if count != 0 {
		if err := c.broker.bufferManager.writeBuffers(stream, topic, buf, count); err != nil {
			return err
		}
	}

	c.broker.subscriptionManager.store(topic, stream)
	c.subStreams[topic] = stream
	return nil
}

func (c *client) unsubscribe(topic string) {
	stream := c.subStreams[topic]
	c.broker.subscriptionManager.delete(topic, stream.StreamID())
	_, _ = stream.Write([]byte{0, 0})
	_ = stream.Close()
}

func (c *client) closeStreams() {
	for topic, stream := range c.subStreams {
		c.broker.subscriptionManager.delete(topic, stream.StreamID())
		_ = stream.Close()
	}
}

func (c *client) read(buf []byte, stream quic.Stream) (int, error) {
	if _, err := stream.Read(buf[:2]); err != nil {
		return 0, err
	}

	size := int(binary.LittleEndian.Uint16(buf))
	if size > c.broker.maxMessageByte {
		return 0, fmt.Errorf("over max message size %v %v", c.broker.maxMessageByte, size)
	}

	sum := 0
	for sum < size {
		n, err := stream.Read(buf[sum:size])
		if err != nil {
			return 0, err
		}

		sum += n
	}

	return size, nil
}
