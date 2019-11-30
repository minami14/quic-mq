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
	broker *MessageBroker
}

func newClient(broker *MessageBroker) *client {
	return &client{broker: broker}
}

func (c *client) run(ctx context.Context, session quic.Session) error {
	stream, err := session.AcceptStream(ctx)
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

	return c.startStream(ctx, buf, stream, session)
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
	endStream
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

func (c *client) startStream(ctx context.Context, buf []byte, stream quic.Stream, session quic.Session) error {
	streams := map[quic.SendStream]string{}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		n, err := c.read(buf, stream)
		if err != nil {
			return err
		}

		if n < 2 {
			for stream, streamName := range streams {
				c.broker.streamManager.delete(streamName, stream.StreamID())
				_ = stream.Close()
			}
			return nil
		}

		messageType := buf[0]
		streamName := string(buf[1:n])
		n, err = c.read(buf, stream)
		if err != nil {
			return err
		}

		switch messageType {
		case publishMessage:
			n = c.broker.publish(streamName, buf[:n])
		case publishBufferedMessage:
			lifetime := time.Duration(binary.LittleEndian.Uint32(buf))
			c.broker.bufferManager.store(streamName, buf[4:n], lifetime)
			n = c.broker.publish(streamName, buf[4:n])
		case deleteBufferedMessage:
			c.broker.bufferManager.delete(streamName, buf[:16])
		case subscribe:
			s, err := c.subscribe(ctx, streamName, buf[:n], session)
			if err != nil {
				return err
			}
			streams[s] = streamName
		case endStream:
			for stream, topic := range streams {
				if topic == streamName {
					c.broker.streamManager.delete(streamName, stream.StreamID())
					_, _ = stream.Write([]byte{0, 0})
					_ = stream.Close()
					break
				}
			}
		default:
			return fmt.Errorf("invalid message %v", buf)
		}
	}
}

func (c *client) subscribe(ctx context.Context, streamName string, requestBuffer []byte, session quic.Session) (quic.SendStream, error) {
	if requestBuffer[0] > 3 {
		return nil, fmt.Errorf("invalid request %v", requestBuffer)
	}

	stream, err := session.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 3+len(streamName))
	binary.LittleEndian.PutUint16(buf, uint16(len(streamName)+1))
	buf[2] = sub
	copy(buf[3:], streamName)
	if _, err := stream.Write(buf); err != nil {
		return nil, err
	}

	var buffers [][]byte
	switch requestBuffer[0] {
	case notRequestBuffer:
		c.broker.streamManager.store(streamName, stream)
		return stream, nil
	case requestBufferTime:
		duration := time.Duration(binary.LittleEndian.Uint32(requestBuffer[1:])) * time.Second
		buffers = c.broker.bufferManager.loadByTime(streamName, duration)
	case requestBufferCount:
		count := int(binary.LittleEndian.Uint16(requestBuffer[1:]))
		buffers = c.broker.bufferManager.loadByCount(streamName, count)
	case requestBufferAll:
		buffers = c.broker.bufferManager.load(streamName)
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
			return nil, err
		}
	}

	c.broker.streamManager.store(streamName, stream)
	return stream, nil
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
