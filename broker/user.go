package broker

import "sync"

type userManger struct {
	*sync.RWMutex
	users map[string]string
}

func newUserManger() *userManger {
	return &userManger{
		RWMutex: new(sync.RWMutex),
		users:   map[string]string{},
	}
}

func (m *userManger) register(uid, password string) {
	m.Lock()
	m.users[uid] = password
	m.Unlock()
}

func (m *userManger) unregister(uid string) {
	m.Lock()
	if _, ok := m.users[uid]; ok {
		delete(m.users, uid)
	}
	m.Unlock()
}

func (m *userManger) verify(uid, password string) bool {
	pw, ok := m.users[uid]
	return ok && password == pw
}
