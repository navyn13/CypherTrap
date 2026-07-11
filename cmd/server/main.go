package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/navyn13/CypherTrap/internal/config"
	"github.com/navyn13/CypherTrap/internal/events"
	"github.com/navyn13/CypherTrap/internal/server"
	"github.com/navyn13/CypherTrap/internal/storage/postgres"
	"github.com/navyn13/CypherTrap/internal/storage/redis"
)

func main() {
	// Load configs
	cfg, err := config.Load()
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Server setup (auth service is shared with Kafka for cache invalidation)
	srv := server.NewServer(cfg, rdb, dbConn)

	kafkaBrokers := []string{cfg.KafkaBroker}
	kafkaTopics := []string{"api-key-events"}
	consumer, err := events.NewConsumer(kafkaBrokers, "cyphertrap-group", kafkaTopics, rdb, dbConn, srv.AuthService(), srv.RateLimitService())
	if err != nil {
		slog.Warn("Kafka consumer failed to start", "err", err)
	} else {
		go func() {
			if err := consumer.Start(ctx); err != nil {
				slog.Error("Kafka consumer error", "err", err)
			}
		}()
		defer consumer.Close()
	}

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	slog.Info("Shutting down server gracefully...")
	cancel() // Stop Kafka consumer
	srv.Shutdown()
	slog.Info("Server stopped.")
}
