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
	ln  net.Listener
	rdb *redis.Client
}

func NewServer(cfg Config, rdb *redis.Client) *Server {
	return &Server{Config: cfg, rdb: rdb}
}
func (s *Server) StartServer() error {
	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		log.Fatal(err)
	}
	s.ln = ln
	slog.Info("CypherTrap Server Running", "listenAddr", s.ListenAddr)
	return nil
}

func (s *Server) Shutdown() {
	if s.ln != nil {
		s.ln.Close()
	}
	if s.rdb != nil {
		s.rdb.Close()
	}
}
