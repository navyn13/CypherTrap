package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ListenAddr string `yaml:"listen_addr"`
	RedisURL   string `yaml:"redis_url"`
	DBURL      string `yaml:"db_url"`
}

func Load(path string) (Config, error) {
	//load env
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	//load config
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	//parse config
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	//set db url
	cfg.DBURL = os.Getenv("DB_URL")
	//set redis url
	cfg.RedisURL = os.Getenv("REDIS_URL")
	//set listen addr
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":7878"
	}
	return cfg, nil
}
