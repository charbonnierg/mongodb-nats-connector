package server

import (
	"context"
	"net"
	"net/http"

	"go.uber.org/zap"
)

const defaultAddr = "127.0.0.1:18080"

type Server struct {
	addr     string
	ctx      context.Context
	monitors []NamedMonitor
	logger   *zap.Logger

	http *http.Server
}

func New(opts ...Option) *Server {
	s := &Server{
		addr:     defaultAddr,
		ctx:      context.Background(),
		monitors: []NamedMonitor{},
		logger:   zap.NewNop(),
	}

	for _, opt := range opts {
		opt(s)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthCheck(s.monitors...))
	s.http = &http.Server{
		Addr:    s.addr,
		Handler: recoverer(mux),
		BaseContext: func(l net.Listener) context.Context {
			return s.ctx
		},
	}

	return s
}

func (s *Server) Run() error {
	s.logger.Info("server started", zap.String("addr", s.addr))
	return s.http.ListenAndServe()
}

func (s *Server) Close() error {
	s.logger.Info("server gracefully shutting down", zap.String("addr", s.addr))
	return s.http.Shutdown(context.Background())
}

type Option func(*Server)

func WithAddr(addr string) Option {
	return func(s *Server) {
		if addr != "" {
			s.addr = addr
		}
	}
}

func WithContext(ctx context.Context) Option {
	return func(s *Server) {
		if ctx != nil {
			s.ctx = ctx
		}
	}
}

func WithNamedMonitors(monitors ...NamedMonitor) Option {
	return func(s *Server) {
		if monitors != nil {
			s.monitors = monitors
		}
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(s *Server) {
		if logger != nil {
			s.logger = logger
		}
	}
}
