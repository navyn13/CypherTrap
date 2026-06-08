package main

import (
	"log"
	"log/slog"
	"net"
)

type Config struct {
	ListenAddr string
}
type Server struct {
	Config
	ln net.Listener
}

func NewServer(cfg Config) *Server {
	return &Server{Config: cfg}
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
	s.ln.Close()
}
