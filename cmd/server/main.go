package main

import (
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
)

func loadEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func main() {
	loadEnv()
	config := Config{
		ListenAddr: os.Getenv("PORT"),
	}
	rdb, err := NewRedisClient()
	if err != nil {
		log.Fatal(err)
	}
	server := NewServer(config, rdb)
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
