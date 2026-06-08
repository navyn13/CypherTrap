package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func NewRedisClient() (*redis.Client, error) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	db, err := strconv.Atoi(os.Getenv("REDIS_DB"))
	if err != nil {
		db = 0
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       db,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return rdb, nil
}

func SetKey(rdb *redis.Client, key string, value string) error {
	err := rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func GetKey(rdb *redis.Client, key string) (string, error) {
	val, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return "", err
	}
	return val, nil
}
