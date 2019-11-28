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
	broker          *MessageBroker
	pubStreamsCount int
}

const (
	pub = iota + 1
	sub
)

func (c *client) run(ctx context.Context, session quic.Session) error {
	for {
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			return err
		}

		buf := make([]byte, 4)
		if _, err := c.read(buf, stream); err != nil {
			return err
		}

		streamType := buf[0]
		switch streamType {
		case pub:
			go func() {
				if err := c.startPubStream(ctx, stream); err != nil {
					c.broker.logger.Println(err)
				}
			}()
		case sub:
			if err := c.startSubStream(ctx, stream); err != nil {
				return err
			}
		default:
			return fmt.Errorf("invalid stream type %v", streamType)
		}
	}
}

const (
	publishMessage = iota + 1
	publishBufferedMessage
	deleteBufferedMessage
)

const (
	statusOK = iota + 1
)

func (c *client) startPubStream(ctx context.Context, stream quic.Stream) error {
	buf := make([]byte, c.broker.maxMessageSize)
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

		if n <= 2 {
			return fmt.Errorf("invalid message %v", buf)
		}

		messageType := buf[0]
		streamName := string(buf[1:n])
		n, err = c.read(buf, stream)
		if err != nil {
			return err
		}

		responseByte := 0
		switch messageType {
		case publishMessage:
			n = c.broker.publish(streamName, buf[:n])
			binary.LittleEndian.PutUint16(buf, uint16(3))
			buf[2] = statusOK
			binary.LittleEndian.PutUint16(buf[3:], uint16(n))
			responseByte = 5
		case publishBufferedMessage:
			lifetime := time.Duration(binary.LittleEndian.Uint32(buf))
			streamID := c.broker.bufferManager.store(streamName, buf[4:], lifetime)
			n = c.broker.publish(streamName, buf[4:n])
			binary.LittleEndian.PutUint16(buf, uint16(19))
			buf[2] = statusOK
			binary.LittleEndian.PutUint16(buf[3:], uint16(n))
			copy(buf[5:21], streamID)
			responseByte = 21
		case deleteBufferedMessage:
			c.broker.bufferManager.delete(streamName, buf[:16])
			binary.LittleEndian.PutUint16(buf, uint16(1))
			buf[2] = statusOK
			responseByte = 3
		default:
			return fmt.Errorf("invalid message %v", buf)
		}

		if _, err := stream.Write(buf[:responseByte]); err != nil {
			return err
		}
	}
}

func (c *client) startSubStream(ctx context.Context, stream quic.Stream) error {
	buf := make([]byte, c.broker.maxMessageSize)
	n, err := stream.Read(buf)
	if err != nil {
		return err
	}

	streamName := string(buf[:n])
	c.broker.subscribe(streamName, stream)
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
