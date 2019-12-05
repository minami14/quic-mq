package broker

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/lucas-clemente/quic-go"
)

type message struct {
	buf      []byte
	createAt time.Time
}

type messageBuffer struct {
	*sync.RWMutex
	messages        []*message
	headIndex       int
	nextIndex       int
	count           int
	maxBuffersCount int
}

func newMessageBuffer(maxBuffersCount int) *messageBuffer {
	return &messageBuffer{
		RWMutex:         new(sync.RWMutex),
		messages:        make([]*message, maxBuffersCount),
		maxBuffersCount: maxBuffersCount,
	}
}

func (b *messageBuffer) store(msg *message) {
	b.Lock()
	defer b.Unlock()
	b.messages[b.nextIndex] = msg
	b.nextIndex++
	if b.nextIndex >= b.maxBuffersCount {
		b.nextIndex = 0
	}

	if b.count < b.maxBuffersCount {
		b.count++
	} else {
		b.headIndex++
		if b.headIndex >= b.maxBuffersCount {
			b.headIndex = 0
		}
	}
}

func (b *messageBuffer) writeBuffer(stream quic.SendStream, buf []byte, count int) error {
	b.RLock()
	defer b.RUnlock()
	if b.count < count {
		count = b.count
	}

	n := 0
	index := b.nextIndex - count
	if index < 0 {
		for i := b.maxBuffersCount + index; i < b.maxBuffersCount; i++ {
			msg := b.messages[i].buf
			if n > 0 && len(msg)+2+n >= len(buf) {
				if _, err := stream.Write(buf[:n]); err != nil {
					return err
				}

				n = 0
			}

			binary.LittleEndian.PutUint16(buf[n:], uint16(len(msg)))
			n += 2
			copy(buf[n:len(msg)+n], msg)
			n += len(msg)
		}
		index = 0
	}

	for index < b.nextIndex {
		msg := b.messages[index].buf
		index++
		if n > 0 && len(msg)+2+n >= len(buf) {
			if _, err := stream.Write(buf[:n]); err != nil {
				return err
			}

			n = 0
		}

		binary.LittleEndian.PutUint16(buf[n:], uint16(len(msg)))
		n += 2
		copy(buf[n:len(msg)+n], msg)
		n += len(msg)
	}

	if n > 0 {
		if _, err := stream.Write(buf[:n]); err != nil {
			return err
		}
	}

	return nil
}

func (b *messageBuffer) deleteExpiredMessages(now time.Time, lifetime time.Duration) {
	b.Lock()
	defer b.Unlock()
	for max := b.maxBuffersCount; b.headIndex < max; b.headIndex++ {
		msg := b.messages[b.headIndex]
		if msg == nil {
			return
		}

		if now.Sub(msg.createAt) < lifetime {
			return
		}

		b.messages[b.headIndex] = nil
		b.headIndex++
		b.count--
		if b.headIndex >= b.maxBuffersCount {
			b.headIndex = 0
			max = b.nextIndex
		}
	}
}

type messageBufferManager struct {
	*sync.RWMutex
	broker          *MessageBroker
	buffers         map[string]*messageBuffer
	lifetime        time.Duration
	maxBuffersCount int
	maxTopicCount   int
}

func newBufferedMessageManager(broker *MessageBroker, config *Config) *messageBufferManager {
	return &messageBufferManager{
		RWMutex:         new(sync.RWMutex),
		broker:          broker,
		buffers:         map[string]*messageBuffer{},
		lifetime:        config.BufferLifetime,
		maxBuffersCount: config.MaxBuffersCountPerTopic,
		maxTopicCount:   config.MaxTopicCount,
	}
}

func (m *messageBufferManager) store(topic string, buf []byte) {
	m.Lock()
	buffers, ok := m.buffers[topic]
	if !ok {
		if len(m.buffers) >= m.maxTopicCount {
			m.Unlock()
			return
		}

		buffers = newMessageBuffer(m.maxBuffersCount)
		m.buffers[topic] = buffers
	}

	m.Unlock()
	cp := make([]byte, len(buf))
	copy(cp, buf)
	msg := &message{
		buf:      cp,
		createAt: time.Now(),
	}

	buffers.store(msg)
	return
}

func (m *messageBufferManager) writeBuffers(stream quic.SendStream, topic string, buf []byte, count int) error {
	m.RLock()
	defer m.RUnlock()
	buffer, ok := m.buffers[topic]
	if ok {
		return buffer.writeBuffer(stream, buf, count)
	}

	return nil
}

func (m *messageBufferManager) deleteExpiredMessages() {
	now := time.Now()
	for _, buffer := range m.buffers {
		buffer.deleteExpiredMessages(now, m.lifetime)
	}

	m.Lock()
	defer m.Unlock()
	var deadBuffers []string
	for topic, buffer := range m.buffers {
		if buffer.count <= 0 {
			deadBuffers = append(deadBuffers, topic)
		}
	}

	for _, topic := range deadBuffers {
		delete(m.buffers, topic)
	}
}
