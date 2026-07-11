package server

import (
	"log/slog"
	"strings"
)

var (
	respInvalid      = []byte("INVALID MESSAGE\n")
	respUnauthorized = []byte("UNAUTHORIZED\n")
	respInternal     = []byte("INTERNAL SERVER ERROR\n")
	respAllowed      = []byte("ALLOWED\n")
	respBlocked      = []byte("BLOCKED\n")
	respUnknown      = []byte("UNKNOWN COMMAND\n")
)

func (s *Server) handleMessage(msg Message) error {
	parts := strings.Fields(msg.Msg)

	if len(parts) != 6 {
		msg.Peer.Send(respInvalid)
		return nil
	}
	command := parts[0]
	ip := parts[1]
	companyName := parts[2]
	key_name := parts[3]
	api_key := parts[4]
	policyName := parts[5]

	isVerified := s.authService.VerifyAPIKey(companyName, key_name, api_key)
	if !isVerified {
		msg.Peer.Send(respUnauthorized)
		return nil
	}

	algorithm, err := s.ratelimitService.LookupAlgorithmAndConfig(companyName, key_name, policyName)
	if err != nil {
		msg.Peer.Send(respInternal)
		return nil
	}

	switch command {
	case "ALLOW":
		if algorithm.Check(ip, companyName, key_name) {
			msg.Peer.Send(respAllowed)
		} else {
			msg.Peer.Send(respBlocked)
		}
	default:
		msg.Peer.Send(respUnknown)
	}
	return nil
}

const (
	messageWorkerCount = 256
	messageQueueSize   = 8192
)

func (s *Server) peerLoop() {
	for {
		select {
		case peer := <-s.addPeerCh:
			s.peers[peer] = true
		case <-s.quitCh:
			return
		}
	}
}

func (s *Server) messageWorkerLoop() {
	for {
		select {
		case msg := <-s.msgCh:
			if err := s.handleMessage(msg); err != nil {
				slog.Error("raw Message Error", "err", err)
			}
		case <-s.quitCh:
			return
		}
	}
}
