package broker

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"time"

	"github.com/lucas-clemente/quic-go"
)

// MessageBroker delivers messages to clients.
type MessageBroker struct {
	maxMessageByte      int
	userManger          *userManger
	subscriptionManager *subscriptionManager
	bufferManager       *messageBufferManager
	logger              *log.Logger
}

// New creates an instance of MessageBroker.
func New(config *Config) *MessageBroker {
	b := &MessageBroker{
		maxMessageByte: config.MaxMessageByte,
		userManger:     newUserManger(),
		logger:         log.New(os.Stdout, "broker ", log.LstdFlags),
	}

	b.subscriptionManager = newStreamManager(b)
	b.bufferManager = newBufferedMessageManager(b, config)
	b.bufferManager.lifetime = config.BufferLifetime
	b.bufferManager.maxBuffersCount = config.MaxBuffersCountPerTopic
	b.bufferManager.maxTopicCount = config.MaxTopicCount
	for _, user := range config.Users {
		b.userManger.register(user.UserID, user.Password)
	}

	return b
}

// Start starts a MessageBroker.
func (b *MessageBroker) Start(ctx context.Context, addr string, tlsConf *tls.Config) error {
	listener, err := quic.ListenAddr(addr, tlsConf, nil)
	if err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.bufferManager.deleteExpiredMessages()
			}
		}
	}()

	for {
		session, err := listener.Accept(ctx)
		if err != nil {
			b.logger.Println(err)
		}

		go func() {
			c := newClient(b, session)
			if err := c.run(ctx); err != nil {
				b.logger.Println(err)
			}
		}()
	}
}
