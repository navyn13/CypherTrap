package config

import (
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	ListenAddr  string
	RedisURL    string
	DBURL       string
	KafkaBroker string
	TLSCertFile string
	TLSKeyFile  string
}

func Load() (Config, error) {
	// load .env if present (optional)
	if err := godotenv.Load(); err != nil {
		slog.Warn("no .env file loaded, relying on environment", "err", err)
	}

	cfg := Config{
		ListenAddr:  getEnv("LISTEN_ADDR", ":7878"),
		RedisURL:    os.Getenv("REDIS_URL"),
		DBURL:       os.Getenv("DB_URL"),
		KafkaBroker: getEnv("KAFKA_BROKER", "localhost:9092"),
		TLSCertFile: getEnv("TLS_CERT_FILE", "certs/server.crt"),
		TLSKeyFile:  getEnv("TLS_KEY_FILE", "certs/server.key"),
	}
	return cfg, nil
}

// getEnv returns the environment variable for key, or fallback if unset/empty.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
