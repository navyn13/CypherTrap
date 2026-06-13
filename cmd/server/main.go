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
	//load configs
	cfg, err := config.Load("./config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	//redis-setup
	rdb, err := NewRedisClient(cfg.RedisURL)
	if err == nil {
		slog.Info("Redis connected\n")
	} else {
		log.Fatal(err)
	}
	defer rdb.Close()
	//postres setup
	db, err := NewDB(cfg.DBURL)
	if err == nil {
		slog.Info("Database connected\n")
	} else {
		log.Fatal(err)
	}
	defer CloseDB(db)
	//server-setup
	server := NewServer(cfg, rdb, db)
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
