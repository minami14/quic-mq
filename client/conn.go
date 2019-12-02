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
	config            *Config
	stream            quic.Stream
	session           quic.Session
	buf               []byte
	mutex             *sync.Mutex
	acceptStreamMutex *sync.Mutex
	logger            *log.Logger
}

const (
	statusOK = iota + 1
)

const (
	startPublish = iota + 1
	cancelPublish
	subscribe
	unsubscribe
)

const (
	pub = iota + 1
	sub
)

// Dial connects to the message broker.
func Dial(ctx context.Context, address string, tlsConf *tls.Config, config *Config) (*Conn, error) {
	conn := &Conn{
		config:            config,
		logger:            log.New(os.Stdout, "client ", log.LstdFlags),
		mutex:             new(sync.Mutex),
		acceptStreamMutex: new(sync.Mutex),
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

// CreatePublishStream creates publish stream.
func (c *Conn) CreatePublishStream(ctx context.Context, topic string) (*PublishStream, error) {
	c.acceptStreamMutex.Lock()
	defer c.acceptStreamMutex.Unlock()
	c.mutex.Lock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = startPublish
	n++
	copy(c.buf[n:], topic)
	n += len(topic)
	if _, err := c.stream.Write(c.buf[:n]); err != nil {
		return nil, err
	}

	c.mutex.Unlock()
	stream, err := c.session.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, c.config.MaxMessageSize)
	n, err = c.read(buf, stream)
	if err != nil {
		return nil, err
	}

	if buf[0] != pub || string(buf[1:n]) != topic {
		return nil, fmt.Errorf("failed to create publish stream %v", topic)
	}

	return newPublishStream(topic, stream, c), nil
}

// Publish publishes a message.
func (c *Conn) Publish(topic string, data []byte, buffer bool, duration time.Duration) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	if buffer {
		c.buf[n] = cancelPublish
		n++
		copy(c.buf[n:], topic)
		n += len(topic)
		binary.LittleEndian.PutUint16(c.buf[n:], uint16(len(data)+4))
		n += 2
		binary.LittleEndian.PutUint32(c.buf[n:], uint32(duration/time.Second))
		n += 4
	} else {
		c.buf[n] = startPublish
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

func (c *Conn) cancelPublish(topic string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = cancelPublish
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

// Subscribe subscribes to a topic.
func (c *Conn) Subscribe(ctx context.Context, topic string, count int) (*SubscribeStream, error) {
	c.acceptStreamMutex.Lock()
	defer c.acceptStreamMutex.Unlock()
	c.mutex.Lock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = subscribe
	n++
	copy(c.buf[n:], topic)
	n += len(topic)
	binary.LittleEndian.PutUint16(c.buf[n:], 2)
	n += 2
	binary.LittleEndian.PutUint16(c.buf[n:], uint16(count))
	n += 2
	if _, err := c.stream.Write(c.buf[:n]); err != nil {
		c.mutex.Unlock()
		return nil, err
	}

	c.mutex.Unlock()
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

	subStream := newSubscribeStream(topic, stream, c)
	go func() {
		if err := subStream.start(ctx); err != nil {
			c.logger.Println(err)
		}
	}()

	return subStream, nil
}

func (c *Conn) unsubscribe(topic string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	binary.LittleEndian.PutUint16(c.buf, uint16(len(topic)+1))
	n := 2
	c.buf[n] = unsubscribe
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
