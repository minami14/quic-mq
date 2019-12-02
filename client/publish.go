package client

import (
	"encoding/binary"
	"sync"

	"github.com/lucas-clemente/quic-go"
)

// PublishStream is a stream for publish message.
type PublishStream struct {
	topic  string
	stream quic.Stream
	conn   *Conn
	buf    []byte
	*sync.Mutex
}

func newPublishStream(topic string, stream quic.Stream, conn *Conn) *PublishStream {
	return &PublishStream{
		topic:  topic,
		stream: stream,
		conn:   conn,
		buf:    make([]byte, conn.config.MaxMessageSize),
		Mutex:  new(sync.Mutex),
	}
}

// PublishFlag is meta information attached to the message to be published.
type PublishFlag byte

// Publish flags
const (
	Buffered PublishFlag = iota + 1
	Unbuffered
)

// Publish publishes message.
func (s *PublishStream) Publish(data []byte, flag PublishFlag) error {
	s.Lock()
	defer s.Unlock()
	binary.LittleEndian.PutUint16(s.buf, uint16(len(data)+1))
	n := 2
	s.buf[n] = byte(flag)
	n++
	copy(s.buf[n:], data)
	n += len(data)
	if _, err := s.stream.Write(s.buf[:n]); err != nil {
		return err
	}

	return nil
}

// Close closes the stream.
func (s *PublishStream) Close() error {
	return s.conn.cancelPublish(s.topic)
}
