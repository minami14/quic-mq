package broker

import (
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type bufferedMessage struct {
	buf      []byte
	create   time.Time
	lifetime time.Duration
}

type bufferedMessageManager struct {
	*sync.RWMutex
	broker   *MessageBroker
	lockers  map[string]*sync.RWMutex
	messages map[string]map[string]*bufferedMessage
}

func newBufferedMessageManager(broker *MessageBroker) *bufferedMessageManager {
	return &bufferedMessageManager{
		RWMutex:  new(sync.RWMutex),
		broker:   broker,
		lockers:  map[string]*sync.RWMutex{},
		messages: map[string]map[string]*bufferedMessage{},
	}
}

func (m *bufferedMessageManager) store(streamName string, buf []byte, duration time.Duration) []byte {
	m.Lock()
	locker, ok := m.lockers[streamName]
	if !ok {
		locker = new(sync.RWMutex)
		m.lockers[streamName] = locker
	}

	messages, ok := m.messages[streamName]
	if !ok {
		messages = map[string]*bufferedMessage{}
		m.messages[streamName] = messages
	}

	m.Unlock()
	cp := make([]byte, len(buf))
	copy(cp, buf)
	messageID := uuid.New()
	message := &bufferedMessage{
		buf:      cp,
		create:   time.Now(),
		lifetime: duration,
	}

	locker.Lock()
	messages[string(messageID[:])] = message
	locker.Unlock()
	return messageID[:]
}

func (m *bufferedMessageManager) loadByTime(streamName string, duration time.Duration) (buffers [][]byte) {
	locker, ok := m.lockers[streamName]
	if !ok {
		return
	}

	messages, ok := m.messages[streamName]
	if !ok {
		return
	}

	locker.RLock()
	for _, message := range messages {
		if time.Now().Sub(message.create) < duration {
			buffers = append(buffers, message.buf)
		}
	}

	locker.RUnlock()
	return
}

func (m *bufferedMessageManager) loadByCount(streamName string, count int) (buffers [][]byte) {
	locker, ok := m.lockers[streamName]
	if !ok {
		return
	}

	messages, ok := m.messages[streamName]
	if !ok {
		return
	}

	locker.RLock()
	var tmpMessages []*bufferedMessage
	for _, message := range messages {
		tmpMessages = append(tmpMessages, message)
	}

	locker.RUnlock()
	sort.Slice(tmpMessages, func(i, j int) bool {
		return tmpMessages[i].create.After(tmpMessages[j].create)
	})

	for i, message := range tmpMessages {
		if i >= count {
			break
		}

		buffers = append(buffers, message.buf)
	}

	return
}

func (m *bufferedMessageManager) load(streamName string) (buffers [][]byte) {
	locker, ok := m.lockers[streamName]
	if !ok {
		return
	}

	messages, ok := m.messages[streamName]
	if !ok {
		return
	}

	locker.RLock()
	for _, message := range messages {
		buffers = append(buffers, message.buf)
	}

	locker.RUnlock()
	return
}

func (m *bufferedMessageManager) delete(streamName string, messageID []byte) {
	locker, ok := m.lockers[streamName]
	if !ok {
		return
	}

	messages, ok := m.messages[streamName]
	if !ok {
		return
	}

	locker.Lock()
	delete(messages, string(messageID))
	locker.Unlock()
}

func (m *bufferedMessageManager) deleteDeadMessages() {
	var deadStreams []string
	for streamName, messages := range m.messages {
		locker, ok := m.lockers[streamName]
		if !ok {
			continue
		}

		var deadMessages []string
		locker.RLock()
		for messageID, message := range messages {
			if time.Now().Sub(message.create) >= message.lifetime {
				deadMessages = append(deadMessages, messageID)
			}
		}

		locker.RUnlock()
		if len(deadMessages) == 0 {
			continue
		}

		locker.Lock()
		for _, messageID := range deadMessages {
			delete(messages, messageID)
		}

		locker.Unlock()
		if len(messages) == 0 {
			deadStreams = append(deadStreams, streamName)
		}
	}

	if len(deadStreams) == 0 {
		return
	}

	m.Lock()
	for _, streamName := range deadStreams {
		delete(m.messages, streamName)
	}

	m.Unlock()
}
