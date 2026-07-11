package server

import (
	"log/slog"
	"strings"
	"time"
)

func (s *Server) handleMessage(msg Message) error {
	parts := strings.Fields(msg.Msg)

	if len(parts) != 6 {
		msg.Peer.Send([]byte("INVALID MESSAGE\n"))
		return nil
	}
	command := parts[0]
	ip := parts[1]
	companyName := parts[2]
	key_name := parts[3]
	api_key := parts[4]
	policyName := parts[5]

	totalStart := time.Now()

	verifyStart := time.Now()
	isVerified := s.authService.VerifyAPIKey(companyName, key_name, api_key)
	verifyDur := time.Since(verifyStart)
	if !isVerified {
		slog.Info("ALLOW process timings",
			"VerifyAPIKey", verifyDur,
			"LookupAlgorithmAndConfig", time.Duration(0),
			"RateLimiterCheck", time.Duration(0),
			"total", time.Since(totalStart),
			"result", "UNAUTHORIZED",
		)
		msg.Peer.Send([]byte("UNAUTHORIZED\n"))
		return nil
	}

	lookupStart := time.Now()
	algorithm, err := s.ratelimitService.LookupAlgorithmAndConfig(companyName, key_name, policyName)
	lookupDur := time.Since(lookupStart)
	if err != nil {
		slog.Info("ALLOW process timings",
			"VerifyAPIKey", verifyDur,
			"LookupAlgorithmAndConfig", lookupDur,
			"RateLimiterCheck", time.Duration(0),
			"total", time.Since(totalStart),
			"result", "INTERNAL SERVER ERROR",
		)
		msg.Peer.Send([]byte("INTERNAL SERVER ERROR\n"))
		return nil
	}

	switch command {
	case "ALLOW":
		checkStart := time.Now()
		allowed := algorithm.Check(ip, companyName, key_name)
		checkDur := time.Since(checkStart)

		result := "BLOCKED"
		if allowed {
			result = "ALLOWED"
			msg.Peer.Send([]byte("ALLOWED\n"))
		} else {
			msg.Peer.Send([]byte("BLOCKED\n"))
		}

		slog.Info("ALLOW process timings",
			"VerifyAPIKey", verifyDur,
			"LookupAlgorithmAndConfig", lookupDur,
			"RateLimiterCheck", checkDur,
			"total", time.Since(totalStart),
			"result", result,
		)
	default:
		msg.Peer.Send([]byte("UNKNOWN COMMAND\n"))
	}
	return nil
}

const messageWorkerCount = 8

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
