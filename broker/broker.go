package broker

import (
	"context"
	"crypto/tls"
	"log"
	"math"

	"github.com/lucas-clemente/quic-go"
)

type MessageBroker struct {
	maxMessageSize int
	maxStreamCount int
	streamManager  *streamManager
	bufferManager  *bufferedMessageManager
	logger         *log.Logger
}

func New() *MessageBroker {
	b := &MessageBroker{
		maxMessageSize: math.MaxInt16,
		maxStreamCount: math.MaxInt8,
		logger:         new(log.Logger),
	}

	b.streamManager = newStreamManager(b)
	b.bufferManager = newBufferedMessageManager(b)

	return b
}

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
			c := &client{
				broker: b,
			}

			if err := c.run(ctx, session); err != nil {
				b.logger.Println(err)
			}
		}()
	}
}

func (b *MessageBroker) publish(streamName string, buf []byte) int {
	return b.streamManager.publish(streamName, buf)
}

func (b *MessageBroker) subscribe(streamName string, stream quic.Stream) {
	b.streamManager.store(streamName, stream)
}
