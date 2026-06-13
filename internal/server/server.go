package server

import (
	"log"
	"log/slog"
	"net"

	"github.com/jackc/pgx/v5"
	"github.com/navyn13/CypherTrap/internal/auth"
	"github.com/navyn13/CypherTrap/internal/config"
	"github.com/navyn13/CypherTrap/internal/ratelimit"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	config.Config
	ln              net.Listener
	rdb             *redis.Client
	addPeerCh       chan *Peer
	peers           map[*Peer]bool
	quitCh          chan struct{}
	msgCh           chan Message
	db              *pgx.Conn
	authService     *auth.Service
	ratelimitService *ratelimit.Service
}

func NewServer(cfg config.Config, rdb *redis.Client, db *pgx.Conn) *Server {
	return &Server{
		Config:           cfg,
		rdb:              rdb,
		db:               db,
		addPeerCh:        make(chan *Peer),
		peers:            make(map[*Peer]bool),
		quitCh:           make(chan struct{}),
		msgCh:            make(chan Message),
		authService:      auth.NewService(db),
		ratelimitService: ratelimit.NewService(db, rdb),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		log.Fatal(err)
	}
	s.ln = ln
	go s.loop()
	slog.Info("CypherTrap Server Running", "listenAddr", s.ListenAddr)
	return s.acceptLoop()
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
