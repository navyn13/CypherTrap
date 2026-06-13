package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/navyn13/CypherTrap/internals/config"
	"github.com/navyn13/CypherTrap/internals/db"
	"github.com/navyn13/CypherTrap/internals/redis"
)

func main() {
	//load configs
	cfg, err := config.Load("./config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	//redis-setup
	rdb, err := redis.NewRedisClient(cfg.RedisURL)
	if err == nil {
		slog.Info("Redis connected\n")
	} else {
		log.Fatal(err)
	}
	defer rdb.Close()
	//postres setup
	dbConn, err := db.NewDB(cfg.DBURL)
	if err == nil {
		slog.Info("Database connected\n")
	} else {
		log.Fatal(err)
	}
	defer db.CloseDB(dbConn)
	//server-setup
	server := NewServer(cfg, rdb, dbConn)
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
