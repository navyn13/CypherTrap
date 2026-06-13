package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/navyn13/CypherTrap/internals/config"
)

func main() {
	cfg, err := config.Load("./config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	rdb, err := NewRedisClient(cfg.RedisAddr, cfg.RedisDB, cfg.RedisPassword)
	if err != nil {
		log.Fatal(err)
	}
	server := NewServer(cfg, rdb)
	go func() {
		if err := server.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	slog.Info("Shutting down server gracefully...")
	server.Shutdown()
	slog.Info("Server stopped.")
}
