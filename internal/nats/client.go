package nats

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"github.com/damianiandrea/mongodb-nats-connector/internal/server"
)

const (
	defaultName = "nats"
)

var (
	ErrClientDisconnected = errors.New("could not reach nats: connection closed")
)

type Client interface {
	server.NamedMonitor
	io.Closer

	AddStream(ctx context.Context, opts *AddStreamOptions) error
	Publish(ctx context.Context, opts *PublishOptions) error
}

type AddStreamOptions struct {
	StreamName string
}

type PublishOptions struct {
	Subj  string
	MsgId string
	Data  []byte
}

var _ Client = &DefaultClient{}

type DefaultClient struct {
	name   string
	logger *zap.Logger

	opts nats.Options
	conn *nats.Conn
	js   nats.JetStreamContext
}

func NewDefaultClient(opts ...ClientOption) (*DefaultClient, error) {
	c := &DefaultClient{
		name:   defaultName,
		logger: zap.NewNop(),
		opts:   nats.GetDefaultOptions(),
	}

	for _, opt := range opts {
		opt(c)
	}
	extraOpts := []nats.Option{
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			c.logger.Error("disconnected from nats", zap.Error(err))
		}),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			c.logger.Error("disconnected from nats", zap.Error(err))
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			c.logger.Info("reconnected to nats", zap.String("url", conn.ConnectedUrlRedacted()))
		}),
		nats.ClosedHandler(func(conn *nats.Conn) {
			c.logger.Info("nats connection closed")
		}),
	}
	for _, extraOpt := range extraOpts {
		if err := extraOpt(&c.opts); err != nil {
			return nil, fmt.Errorf("failed to register handler: %w", err)
		}
	}
	conn, err := c.opts.Connect()
	if err != nil {
		return nil, fmt.Errorf("could not connect to nats: %v", err)
	}
	c.conn = conn

	js, _ := conn.JetStream()
	c.js = js

	c.logger.Info("connected to nats", zap.String("url", conn.ConnectedUrlRedacted()))
	return c, nil
}

func (c *DefaultClient) Name() string {
	return c.name
}

func (c *DefaultClient) Monitor(_ context.Context) error {
	if closed := c.conn.IsClosed(); closed {
		return ErrClientDisconnected
	}
	return nil
}

func (c *DefaultClient) Close() error {
	c.conn.Close()
	return nil
}

func (c *DefaultClient) AddStream(_ context.Context, opts *AddStreamOptions) error {
	_, err := c.js.AddStream(&nats.StreamConfig{
		Name:     opts.StreamName,
		Subjects: []string{fmt.Sprintf("%s.*", opts.StreamName)},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("could not add nats stream %v: %v", opts.StreamName, err)
	}
	c.logger.Debug("added nats stream", zap.String("streamName", opts.StreamName))
	return nil
}

func (c *DefaultClient) Publish(_ context.Context, opts *PublishOptions) error {
	if _, err := c.js.Publish(opts.Subj, opts.Data, nats.MsgId(opts.MsgId)); err != nil {
		return fmt.Errorf("could not publish message %v to nats stream %v: %v", opts.Data, opts.Subj, err)
	}
	c.logger.Debug("published message", zap.String("subj", opts.Subj), zap.String("data", string(opts.Data)))
	return nil
}

type ClientOption func(*DefaultClient) error

func WithNatsUrl(url string) ClientOption {
	return func(c *DefaultClient) error {
		if url != "" {
			c.opts.Servers = strings.Split(url, ",")
		}
		return nil
	}
}

func WithNatsOptions(opts ...nats.Option) ClientOption {
	return func(c *DefaultClient) error {
		for _, opt := range opts {
			if err := opt(&c.opts); err != nil {
				return err
			}
		}
		return nil
	}
}

func WithLogger(logger *zap.Logger) ClientOption {
	return func(c *DefaultClient) error {
		if logger != nil {
			c.logger = logger
		}
		return nil
	}
}
