package server

import (
	"log/slog"
	"strings"
)

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

	isVerified := s.authService.VerifyAPIKey(namespace, key_name, api_key)
	if !isVerified {
		msg.Peer.Send([]byte("UNAUTHORIZED\n"))
		return nil
	}

	algorithm, err := s.ratelimitService.LookupAlgorithmAndConfig(namespace, key_name)

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
