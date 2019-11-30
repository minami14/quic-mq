package client

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/lucas-clemente/quic-go"
)

// Conn represents a connection to a message broker.
type Conn struct {
	config  *Config
	stream  quic.Stream
	session quic.Session
	buf     []byte
	mu      *sync.Mutex
	subMu   *sync.Mutex
	logger  *log.Logger
}

const (
	statusOK = iota + 1
)

const (
	publishMessage = iota + 1
	publishBufferedMessage
	_ // deleteBufferedMessage
	subscribe
	endStream
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

// Dial connects to the message broker.
func Dial(ctx context.Context, address string, tlsConf *tls.Config, config *Config) (*Conn, error) {
	conn := &Conn{
		config: config,
		logger: log.New(os.Stdout, "client ", log.LstdFlags),
		mu:     new(sync.Mutex),
		subMu:  new(sync.Mutex),
	}

	session, err := quic.DialAddrContext(ctx, address, tlsConf, nil)
	if err != nil {
		return nil, err
	}

	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, config.MaxMessageSize)
	binary.LittleEndian.PutUint16(buf, uint16(len(config.UserID)))
	n := 2
	copy(buf[n:], config.UserID)
	n += len(config.UserID)
	binary.LittleEndian.PutUint16(buf[n:], uint16(len(config.Password)))
	n += 2
	copy(buf[n:], config.Password)
	n += len(config.Password)
	if _, err := stream.Write(buf[:n]); err != nil {
		_ = stream.Close()
		_ = session.Close()
		return nil, err
	}

	n, err = conn.read(buf, stream)
	if err != nil {
		return nil, err
	}

	if n != 1 || buf[0] != statusOK {
		return nil, fmt.Errorf("authentication failure")
	}

	conn.stream = stream
	conn.session = session
	conn.buf = buf

	return conn, err
}

// Publish publishes a message.
func (c *Conn) Publish(topic string, data []byte, buffer bool, duration time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	if buffer {
		c.buf[n] = publishBufferedMessage
		n++
		copy(c.buf[n:], topic)
		n += len(topic)
		binary.LittleEndian.PutUint16(c.buf[n:], uint16(len(data)+4))
		n += 2
		binary.LittleEndian.PutUint32(c.buf[n:], uint32(duration/time.Second))
		n += 4
	} else {
		c.buf[n] = publishMessage
		n++
		copy(c.buf[n:], topic)
		n += len(topic)
		binary.LittleEndian.PutUint16(c.buf[n:], uint16(len(data)))
		n += 2
	}

	copy(c.buf[n:], data)
	n += len(data)
	if _, err := c.stream.Write(c.buf[:n]); err != nil {
		return err
	}

	return nil
}

// Subscribe subscribes to a topic.
func (c *Conn) Subscribe(ctx context.Context, topic string) (*SubConn, error) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.mu.Lock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = subscribe
	n++
	copy(c.buf[n:], topic)
	n += len(topic)
	binary.LittleEndian.PutUint16(c.buf[n:], 1)
	n += 2
	c.buf[n] = notRequestBuffer
	n++
	if _, err := c.stream.Write(c.buf[:n]); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	c.mu.Unlock()
	stream, err := c.session.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, c.config.MaxMessageSize)
	n, err = c.read(buf, stream)
	if err != nil {
		return nil, err
	}

	if buf[0] != sub || string(buf[1:n]) != topic {
		return nil, fmt.Errorf("failed to subscribe %v", topic)
	}

	subConn := newSubConn(topic, stream, c)
	go func() {
		if err := subConn.start(ctx); err != nil {
			c.logger.Println(err)
		}
	}()

	return subConn, nil
}

// SubscribeRequestAllBuffer subscribes to a topic and requests all buffers.
func (c *Conn) SubscribeRequestAllBuffer(ctx context.Context, topic string) (*SubConn, error) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.mu.Lock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = subscribe
	n++
	copy(c.buf[n:], topic)
	n += len(topic)
	binary.LittleEndian.PutUint16(c.buf[n:], 1)
	n += 2
	c.buf[n] = requestBufferAll
	n++
	if _, err := c.stream.Write(c.buf[:n]); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	c.mu.Unlock()
	stream, err := c.session.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, c.config.MaxMessageSize)
	n, err = c.read(buf, stream)
	if err != nil {
		return nil, err
	}

	if buf[0] != sub || string(buf[1:n]) != topic {
		return nil, fmt.Errorf("failed to subscribe %v", topic)
	}

	subConn := newSubConn(topic, stream, c)
	go func() {
		if err := subConn.start(ctx); err != nil {
			c.logger.Println(err)
		}
	}()

	return subConn, nil
}

// SubscribeRequestBufferByDuration subscribes to a topic and requests buffers before specified duration.
func (c *Conn) SubscribeRequestBufferByDuration(ctx context.Context, topic string, duration time.Duration) (*SubConn, error) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.mu.Lock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = subscribe
	n++
	copy(c.buf[n:], topic)
	n += len(topic)
	binary.LittleEndian.PutUint16(c.buf[n:], 5)
	n += 2
	c.buf[n] = requestBufferTime
	n++
	binary.LittleEndian.PutUint32(c.buf[n:], uint32(duration))
	n += 4
	if _, err := c.stream.Write(c.buf[:n]); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	c.mu.Unlock()
	stream, err := c.session.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, c.config.MaxMessageSize)
	n, err = c.read(buf, stream)
	if err != nil {
		return nil, err
	}

	if buf[0] != sub || string(buf[1:n]) != topic {
		return nil, fmt.Errorf("failed to subscribe %v", topic)
	}

	subConn := newSubConn(topic, stream, c)
	go func() {
		if err := subConn.start(ctx); err != nil {
			c.logger.Println(err)
		}
	}()

	return subConn, nil
}

// SubscribeRequestBufferByCount subscribes to a topic and requests the specified count of buffers.
func (c *Conn) SubscribeRequestBufferByCount(ctx context.Context, topic string, count int) (*SubConn, error) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	c.mu.Lock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = subscribe
	n++
	copy(c.buf[n:], topic)
	n += len(topic)
	binary.LittleEndian.PutUint16(c.buf[n:], 3)
	n += 2
	c.buf[n] = requestBufferCount
	n++
	binary.LittleEndian.PutUint16(c.buf[n:], uint16(count))
	n += 2
	if _, err := c.stream.Write(c.buf[:n]); err != nil {
		c.mu.Unlock()
		return nil, err
	}

	c.mu.Unlock()
	stream, err := c.session.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, c.config.MaxMessageSize)
	n, err = c.read(buf, stream)
	if err != nil {
		return nil, err
	}

	if buf[0] != sub || string(buf[1:n]) != topic {
		return nil, fmt.Errorf("failed to subscribe %v", topic)
	}

	subConn := newSubConn(topic, stream, c)
	go func() {
		if err := subConn.start(ctx); err != nil {
			c.logger.Println(err)
		}
	}()

	return subConn, nil
}

// CancelSubscribe cancels subscription.
func (c *Conn) CancelSubscribe(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = endStream
	n++
	copy(c.buf, topic)
	n += len(topic)
	binary.LittleEndian.PutUint16(c.buf[n:], 0)
	n += 2
	if _, err := c.stream.Write(c.buf[:n]); err != nil {
		return err
	}

	return nil
}

// SetLogger is setter for logger.
func (c *Conn) SetLogger(logger *log.Logger) {
	c.logger = logger
}

func (c *Conn) read(buf []byte, reader io.Reader) (int, error) {
	if _, err := reader.Read(buf[:2]); err != nil {
		return 0, err
	}

	size := int(binary.LittleEndian.Uint16(buf))
	if size > c.config.MaxMessageSize {
		return 0, fmt.Errorf("over max message size %v %v", c.config.MaxMessageSize, size)
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
