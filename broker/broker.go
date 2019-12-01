package broker

import (
	"context"
	"crypto/tls"
	"log"
	"math"
	"os"

	"github.com/lucas-clemente/quic-go"
)

// MessageBroker delivers messages to clients.
type MessageBroker struct {
	maxMessageSize      int
	maxStreamCount      int
	userManger          *userManger
	subscriptionManager *subscriptionManager
	bufferManager       *bufferedMessageManager
	logger              *log.Logger
}

// New creates an instance of MessageBroker.
func New() *MessageBroker {
	b := &MessageBroker{
		maxMessageSize: math.MaxInt16,
		maxStreamCount: math.MaxInt8,
		userManger:     newUserManger(),
		logger:         log.New(os.Stdout, "broker ", log.LstdFlags),
	}

	b.subscriptionManager = newStreamManager(b)
	b.bufferManager = newBufferedMessageManager(b)

	return b
}

// SetConfig sets config.
func (b *MessageBroker) SetConfig(config *Config) {
	for _, user := range config.Users {
		b.userManger.register(user.UserID, user.Password)
	}
}

// Start starts a MessageBroker.
func (b *MessageBroker) Start(ctx context.Context, addr string, tlsConf *tls.Config) error {
	listener, err := quic.ListenAddr(addr, tlsConf, nil)
	if err != nil {
		return err
	}

	for {
		session, err := listener.Accept(ctx)
		if err != nil {
			return err
		}

		go func() {
			c := newClient(b)
			if err := c.run(ctx, session); err != nil {
				b.logger.Println(err)
			}
		}()
	}
}
