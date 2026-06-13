package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/navyn13/CypherTrap/internal/config"
	"github.com/navyn13/CypherTrap/internal/server"
	"github.com/navyn13/CypherTrap/internal/storage/postgres"
	"github.com/navyn13/CypherTrap/internal/storage/redis"
)

func main() {
	// Load configs
	cfg, err := config.Load("./config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// Redis setup
	rdb, err := redis.NewRedisClient(cfg.RedisURL)
	if err == nil {
		slog.Info("Redis connected\n")
	} else {
		log.Fatal(err)
	}
	defer rdb.Close()

	// Postgres setup
	dbConn, err := postgres.NewDB(cfg.DBURL)
	if err == nil {
		slog.Info("Database connected\n")
	} else {
		log.Fatal(err)
	}
	defer postgres.CloseDB(dbConn)

	// Server setup
	srv := server.NewServer(cfg, rdb, dbConn)
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	slog.Info("Shutting down server gracefully...")
	srv.Shutdown()
	slog.Info("Server stopped.")
}
