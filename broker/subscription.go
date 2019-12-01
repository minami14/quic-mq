package broker

import (
	"encoding/binary"
	"sync"

	"github.com/lucas-clemente/quic-go"
)

type subscriptionManager struct {
	*sync.RWMutex
	broker  *MessageBroker
	lockers map[string]*sync.RWMutex
	streams map[string]map[quic.StreamID]quic.SendStream
}

func newStreamManager(broker *MessageBroker) *subscriptionManager {
	return &subscriptionManager{
		RWMutex: new(sync.RWMutex),
		broker:  broker,
		lockers: map[string]*sync.RWMutex{},
		streams: map[string]map[quic.StreamID]quic.SendStream{},
	}
}

func (m *subscriptionManager) store(topic string, stream quic.SendStream) {
	m.RLock()
	locker, ok := m.lockers[topic]
	if !ok {
		locker = new(sync.RWMutex)
		m.lockers[topic] = locker
		m.streams[topic] = map[quic.StreamID]quic.SendStream{}
	}

	m.RUnlock()
	locker.Lock()
	streams := m.streams[topic]
	if stream, ok := streams[stream.StreamID()]; ok {
		_ = stream.Close()
	}
	streams[stream.StreamID()] = stream
	locker.Unlock()
}

func (m *subscriptionManager) delete(topic string, streamID quic.StreamID) {
	locker, ok := m.lockers[topic]
	if !ok {
		return
	}

	locker.Lock()
	streams := m.streams[topic]
	delete(streams, streamID)
	if len(streams) == 0 {
		m.Lock()
		delete(m.streams, topic)
		delete(m.lockers, topic)
		m.Unlock()
	}
	locker.Unlock()
}

func (m *subscriptionManager) publish(topic string, message []byte) int {
	locker, ok := m.lockers[topic]
	if !ok {
		return 0
	}

	buf := make([]byte, len(message)+2)
	binary.LittleEndian.PutUint16(buf, uint16(len(message)))
	copy(buf[2:], message)
	locker.RLock()
	count := 0
	var deadStreams []quic.StreamID
	streams := m.streams[topic]
	for id, stream := range streams {
		if _, err := stream.Write(buf); err != nil {
			m.broker.logger.Println(err)
			deadStreams = append(deadStreams, id)
		} else {
			count++
		}
	}

	for _, id := range deadStreams {
		delete(streams, id)
	}

	if len(streams) == 0 {
		m.Lock()
		delete(m.streams, topic)
		delete(m.lockers, topic)
		m.Unlock()
	}

	locker.RUnlock()

	return count
}
