package main

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"strings"

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
	algorithm Algorithm
}

func NewServer(cfg config.Config, rdb *redis.Client) *Server {
	return &Server{
		Config:    cfg,
		rdb:       rdb,
		addPeerCh: make(chan *Peer),
		peers:     make(map[*Peer]bool),
		quitCh:    make(chan struct{}),
		msgCh:     make(chan Message),
		algorithm: NewFixedWindowAlgorithm(5),
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

	if len(parts) != 2 {
		return fmt.Errorf("invalid message")
	}
	command := parts[0]
	ip := parts[1]
	fmt.Println("Command:", command)
	fmt.Println("IP:", ip)

	switch command {
	case "ALLOW":
		algorithm := s.algorithm
		if algorithm.Check(ip) {
			msg.Peer.conn.Write([]byte("ALLOWED\n"))
		} else {
			msg.Peer.conn.Write([]byte("BLOCKED\n"))
		}
	default:
		return fmt.Errorf("unknown command: %s", command)
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
