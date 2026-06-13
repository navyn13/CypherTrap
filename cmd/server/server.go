package main

import (
	"log"
	"log/slog"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/navyn13/CypherTrap/internals/config"
	"github.com/redis/go-redis/v9"
)

type Message struct {
	Peer *Peer
	Msg  string
}

type Server struct {
	config.Config
	ln        net.Listener
	rdb       *redis.Client
	addPeerCh chan *Peer
	peers     map[*Peer]bool
	quitCh    chan struct{}
	msgCh     chan Message
	db        *pgx.Conn
}

func NewServer(cfg config.Config, rdb *redis.Client, db *pgx.Conn) *Server {
	return &Server{
		Config:    cfg,
		rdb:       rdb,
		db:        db,
		addPeerCh: make(chan *Peer),
		peers:     make(map[*Peer]bool),
		quitCh:    make(chan struct{}),
		msgCh:     make(chan Message),
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
func (s *Server) loop() {
	for {
		select {
		case peer := <-s.addPeerCh:
			s.peers[peer] = true
		case msg := <-s.msgCh:
			if err := s.handleMessage(msg); err != nil {
				slog.Error("raw Message Error", "err", err)
			}
		case <-s.quitCh:
			return
		}
	}
}
func (s *Server) handleMessage(msg Message) error {
	parts := strings.Fields(msg.Msg)

	if len(parts) != 5 {
		msg.Peer.Send([]byte("INVALID MESSAGE\n"))
		return nil
	}
	command := parts[0]
	ip := parts[1]
	namespace := parts[2]
	key_name := parts[3]
	api_key := parts[4]

	isVerified := s.verifyAPIKey(namespace, key_name, api_key)
	if !isVerified {
		msg.Peer.Send([]byte("UNAUTHORIZED\n"))
		return nil
	}

	algorithm, err := s.lookupAlgorithmAndConfig(namespace, key_name)

	if err != nil {
		msg.Peer.Send([]byte("INTERNAL SERVER ERROR\n"))
		return nil
	}

	switch command {
	case "ALLOW":
		if algorithm.Check(ip, namespace, key_name) {
			msg.Peer.Send([]byte("ALLOWED\n"))
		} else {
			msg.Peer.Send([]byte("BLOCKED\n"))
		}
	default:
		msg.Peer.Send([]byte("UNKNOWN COMMAND\n"))
	}
	return nil
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
