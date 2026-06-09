package main

import (
	"log"
	"log/slog"
	"net"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	ListenAddr string
}
type Server struct {
	Config
	ln        net.Listener
	rdb       *redis.Client
	addPeerCh chan *Peer
	peers     map[*Peer]bool
	quitCh    chan struct{}
}

func NewServer(cfg Config, rdb *redis.Client) *Server {
	return &Server{Config: cfg, rdb: rdb, addPeerCh: make(chan *Peer), peers: make(map[*Peer]bool), quitCh: make(chan struct{})}
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
func (s *Server) loop() {
	for {
		select {
		case peer := <-s.addPeerCh:
			s.peers[peer] = true
		case <-s.quitCh:
			return
		}
	}
}

func (s *Server) acceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			continue
		}
		go s.handleConn(conn)
	}
}
func (s *Server) handleConn(conn net.Conn) {
	peer := NewPeer(conn)
	s.addPeerCh <- peer
	go peer.readLoop()
}
