package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Algorithm interface {
	Check(ip, companyName, keyName string) bool
}

type blockedCacheEntry struct {
	expiresAt time.Time
}

type fixedWindowAlgorithm struct {
	config       fixedWindowConfig
	rdb          *redis.Client
	blockedCache sync.Map // map[string]blockedCacheEntry — L1
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

	// L1: local blocked cache
	if a.localBlocked(key) {
		return false
	}

	// L2: Redis fixed-window counter
	ttlMs, err := a.rdb.Eval(context.Background(), `
		local current = redis.call('INCR', KEYS[1])

		-- First request in this window
		if current == 1 then
			redis.call('PEXPIRE', KEYS[1], ARGV[2])
		end

		-- Exceeded limit: return remaining TTL (ms) so callers can cache the block
		if current > tonumber(ARGV[1]) then
			local ttl = redis.call('PTTL', KEYS[1])
			if ttl < 0 then
				ttl = tonumber(ARGV[2])
			end
			return ttl
		end

		-- Allowed
		return 0
	`, []string{key}, limit, windowMs).Int()

	if err != nil {
		return false
	}

	if ttlMs > 0 {
		a.blockedCache.Store(key, blockedCacheEntry{
			expiresAt: time.Now().Add(time.Duration(ttlMs) * time.Millisecond),
		})
		return false
	}

	return true
}

func (a *fixedWindowAlgorithm) localBlocked(key string) bool {
	v, ok := a.blockedCache.Load(key)
	if !ok {
		return false
	}
	entry := v.(blockedCacheEntry)
	if time.Now().After(entry.expiresAt) {
		a.blockedCache.Delete(key)
		return false
	}
	return true
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
