package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/lucas-clemente/quic-go"
)

type client struct {
	broker  *MessageBroker
	streams map[quic.SendStream]string
	session quic.Session
}

func newClient(broker *MessageBroker, session quic.Session) *client {
	return &client{
		broker:  broker,
		streams: map[quic.SendStream]string{},
		session: session,
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

	buf := make([]byte, c.broker.maxMessageSize)
	if err := c.verify(buf, stream); err != nil {
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

	return c.startStream(ctx, buf, stream)
}

func (c *client) verify(buf []byte, stream quic.Stream) error {
	n, err := c.read(buf, stream)
	if err != nil {
		return err
	}

	uid := string(buf[:n])
	n, err = c.read(buf, stream)
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
	publishMessage = iota + 1
	publishBufferedMessage
	deleteBufferedMessage
	subscribe
	unsubscribe
)

const (
	statusOK = iota + 1
	authErr
	invalidMessage
)

const (
	notRequestBuffer = iota + 1
	requestBufferTime
	requestBufferCount
	requestBufferAll
)

const (
	_ = iota + 1
	sub
)

func (c *client) startStream(ctx context.Context, buf []byte, stream quic.Stream) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		n, err := c.read(buf, stream)
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
		n, err = c.read(buf, stream)
		if err != nil {
			return err
		}

		switch messageType {
		case publishMessage:
			c.broker.subscriptionManager.publish(topic, buf[:n])
		case publishBufferedMessage:
			lifetime := time.Duration(binary.LittleEndian.Uint32(buf))
			c.broker.bufferManager.store(topic, buf[4:n], lifetime)
			c.broker.subscriptionManager.publish(topic, buf[4:n])
		case deleteBufferedMessage:
			c.broker.bufferManager.delete(topic, buf[:16])
		case subscribe:
			if err := c.subscribe(ctx, topic, buf[:n]); err != nil {
				return err
			}
		case unsubscribe:
			c.unsubscribe(topic)
		default:
			return fmt.Errorf("invalid message %v", buf)
		}
	}
}

func (c *client) subscribe(ctx context.Context, topic string, requestBuffer []byte) error {
	if requestBuffer[0] > 3 {
		return fmt.Errorf("invalid request %v", requestBuffer)
	}

	stream, err := c.session.OpenUniStreamSync(ctx)
	if err != nil {
		return err
	}

	buf := make([]byte, 3+len(topic))
	binary.LittleEndian.PutUint16(buf, uint16(len(topic)+1))
	buf[2] = sub
	copy(buf[3:], topic)
	if _, err := stream.Write(buf); err != nil {
		return err
	}

	var buffers [][]byte
	switch requestBuffer[0] {
	case notRequestBuffer:
		c.broker.subscriptionManager.store(topic, stream)
		return nil
	case requestBufferTime:
		duration := time.Duration(binary.LittleEndian.Uint32(requestBuffer[1:])) * time.Second
		buffers = c.broker.bufferManager.loadByTime(topic, duration)
	case requestBufferCount:
		count := int(binary.LittleEndian.Uint16(requestBuffer[1:]))
		buffers = c.broker.bufferManager.loadByCount(topic, count)
	case requestBufferAll:
		buffers = c.broker.bufferManager.load(topic)
	}

	var buffer []byte
	for _, message := range buffers {
		binary.LittleEndian.PutUint16(buf, uint16(len(message)))
		buffer = append(buffer, buf[:2]...)
		buffer = append(buffer, message...)
	}

	if len(buffer) > 2 {
		if _, err := stream.Write(buffer); err != nil {
			_ = stream.Close()
			return err
		}
	}

	c.broker.subscriptionManager.store(topic, stream)
	c.streams[stream] = topic
	return nil
}

func (c *client) unsubscribe(topic string) {
	for stream, t := range c.streams {
		if t == topic {
			c.broker.subscriptionManager.delete(topic, stream.StreamID())
			_, _ = stream.Write([]byte{0, 0})
			_ = stream.Close()
			return
		}
	}
}

func (c *client) closeStreams() {
	for stream, topic := range c.streams {
		c.broker.subscriptionManager.delete(topic, stream.StreamID())
		_ = stream.Close()
	}
}

func (c *client) read(buf []byte, reader io.Reader) (int, error) {
	if _, err := reader.Read(buf[:2]); err != nil {
		return 0, err
	}

	size := int(binary.LittleEndian.Uint16(buf))
	if size > c.broker.maxMessageSize {
		return 0, fmt.Errorf("over max message size %v %v", c.broker.maxMessageSize, size)
	}

	sum := 0
	for sum < size {
		n, err := reader.Read(buf[sum:size])
		if err != nil {
			return 0, err
		}

		sum += n
	}

	return size, nil
}
