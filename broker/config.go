package broker

import "time"

// Config is MessageBroker config.
type Config struct {
	Users                   []User
	MaxMessageByte          int
	MaxTopicCount           int
	MaxBuffersCountPerTopic int
	BufferLifetime          time.Duration
}

// User is information used for authentication.
type User struct {
	UserID   string
	Password string
}
