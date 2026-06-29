package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Algorithm interface {
	Check(ip, companyName, keyName string) bool
}

type fixedWindowAlgorithm struct {
	config fixedWindowConfig
	rdb    *redis.Client
}

type fixedWindowConfig struct {
	Limit    int `json:"limit"`
	WindowMs int `json:"windowMs"`
}

func NewFixedWindowAlgorithm(config fixedWindowConfig, rdb *redis.Client) Algorithm {
	return &fixedWindowAlgorithm{
		config: config,
		rdb:    rdb,
	}
}

func (a *fixedWindowAlgorithm) Check(ip, companyName, keyName string) bool {
	limit := a.config.Limit
	windowMs := a.config.WindowMs
	key := ip + companyName + keyName

	result, err := a.rdb.Eval(context.Background(), `
		local current = redis.call('INCR', KEYS[1])

		-- First request in this window
		if current == 1 then
			redis.call('PEXPIRE', KEYS[1], ARGV[2])
		end

		-- Exceeded limit
		if current > tonumber(ARGV[1]) then
			return 0
		end

		return 1
	`, []string{key}, limit, windowMs).Int()

	if err != nil {
		return false
	}

	return result == 1
}

func NewAlgorithmFromDB(name string, config json.RawMessage, rdb *redis.Client) (Algorithm, error) {
	switch name {
	case "fixed_window":
		var fixedWindowConfig fixedWindowConfig
		if err := json.Unmarshal(config, &fixedWindowConfig); err != nil {
			return nil, fmt.Errorf("unmarshal fixed window config: %w", err)
		}
		return NewFixedWindowAlgorithm(fixedWindowConfig, rdb), nil
	default:
		return nil, fmt.Errorf("unknown algorithm: %s", name)
	}
}
