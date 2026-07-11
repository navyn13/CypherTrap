package server

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/navyn13/CypherTrap/internal/auth"
	"github.com/navyn13/CypherTrap/internal/config"
	"github.com/navyn13/CypherTrap/internal/ratelimit"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	config.Config
	ln               net.Listener
	rdb              *redis.Client
	addPeerCh        chan *Peer
	peers            map[*Peer]bool
	quitCh           chan struct{}
	msgCh            chan Message
	db               *pgxpool.Pool
	authService      *auth.Service
	ratelimitService *ratelimit.Service
}

func NewServer(cfg config.Config, rdb *redis.Client, db *pgxpool.Pool) *Server {
	return &Server{
		Config:           cfg,
		rdb:              rdb,
		db:               db,
		addPeerCh:        make(chan *Peer),
		peers:            make(map[*Peer]bool),
		quitCh:           make(chan struct{}),
		msgCh:            make(chan Message),
		authService:      auth.NewService(db, rdb),
		ratelimitService: ratelimit.NewService(db, rdb),
	}
}

func (s *Server) Start() error {
	cert, err := tls.LoadX509KeyPair(s.TLSCertFile, s.TLSKeyFile)
	if err != nil {
		return fmt.Errorf("load TLS key pair: %w", err)
	}
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	ln, err := tls.Listen("tcp", s.ListenAddr, tlsConfig)
	if err != nil {
		return fmt.Errorf("tls listen: %w", err)
	}
	s.ln = ln
	go s.peerLoop()
	for range messageWorkerCount {
		go s.messageWorkerLoop()
	}
	slog.Info("CypherTrap Server Running (TLS)", "listenAddr", s.ListenAddr, "messageWorkers", messageWorkerCount)
	return s.acceptLoop()
}

func (s *Server) AuthService() *auth.Service {
	return s.authService
}

func (s *Server) RateLimitService() *ratelimit.Service {
	return s.ratelimitService
}

func (s *Server) Shutdown() {
	if s.ln != nil {
		s.ln.Close()
	}
	if s.rdb != nil {
		s.rdb.Close()
	}
}

func (s *Server) acceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			continue
		}
		go s.handleConn(conn, s.msgCh)
	}
}

func (s *Server) handleConn(conn net.Conn, msgCh chan Message) {
	peer := NewPeer(conn, msgCh)
	s.addPeerCh <- peer
	go peer.readLoop()
}
