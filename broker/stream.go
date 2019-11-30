package broker

import (
	"encoding/binary"
	"sync"

	"github.com/lucas-clemente/quic-go"
)

type streamManager struct {
	*sync.RWMutex
	broker  *MessageBroker
	lockers map[string]*sync.RWMutex
	streams map[string]map[quic.StreamID]quic.SendStream
}

func newStreamManager(broker *MessageBroker) *streamManager {
	return &streamManager{
		RWMutex: new(sync.RWMutex),
		broker:  broker,
		lockers: map[string]*sync.RWMutex{},
		streams: map[string]map[quic.StreamID]quic.SendStream{},
	}
}

func (m *streamManager) store(streamName string, stream quic.SendStream) {
	m.RLock()
	locker, ok := m.lockers[streamName]
	if !ok {
		locker = new(sync.RWMutex)
		m.lockers[streamName] = locker
		m.streams[streamName] = map[quic.StreamID]quic.SendStream{}
	}

	m.RUnlock()
	locker.Lock()
	streams := m.streams[streamName]
	if stream, ok := streams[stream.StreamID()]; ok {
		_ = stream.Close()
	}
	streams[stream.StreamID()] = stream
	locker.Unlock()
}

func (m *streamManager) delete(streamName string, streamID quic.StreamID) {
	locker, ok := m.lockers[streamName]
	if !ok {
		return
	}

	locker.Lock()
	streams := m.streams[streamName]
	delete(streams, streamID)
	if len(streams) == 0 {
		m.Lock()
		delete(m.streams, streamName)
		delete(m.lockers, streamName)
		m.Unlock()
	}
	locker.Unlock()
}

func (m *streamManager) publish(streamName string, message []byte) int {
	locker, ok := m.lockers[streamName]
	if !ok {
		return 0
	}

	buf := make([]byte, len(message)+2)
	binary.LittleEndian.PutUint16(buf, uint16(len(message)))
	copy(buf[2:], message)
	locker.RLock()
	count := 0
	var deadStreams []quic.StreamID
	streams := m.streams[streamName]
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
		delete(m.streams, streamName)
		delete(m.lockers, streamName)
		m.Unlock()
	}

	locker.RUnlock()

	return count
}
