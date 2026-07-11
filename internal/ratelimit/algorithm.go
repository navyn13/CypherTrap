package ratelimit

import (
	"encoding/json"
	"fmt"
)

type Algorithm interface {
	Check(ip, companyName, keyName string) bool
}

type fixedWindowAlgorithm struct {
	config  fixedWindowConfig
	batcher *CheckBatcher
}

type fixedWindowConfig struct {
	Limit    int `json:"limit"`
	WindowMs int `json:"windowMs"`
}

func NewFixedWindowAlgorithm(config fixedWindowConfig, batcher *CheckBatcher) Algorithm {
	return &fixedWindowAlgorithm{
		config:  config,
		batcher: batcher,
	}
}

func (a *fixedWindowAlgorithm) Check(ip, companyName, keyName string) bool {
	key := ip + companyName + keyName
	return a.batcher.Submit(key, a.config.Limit, a.config.WindowMs)
}

func NewAlgorithmFromDB(name string, config json.RawMessage, batcher *CheckBatcher) (Algorithm, error) {
	switch name {
	case "fixed_window":
		var fixedWindowConfig fixedWindowConfig
		if err := json.Unmarshal(config, &fixedWindowConfig); err != nil {
			return nil, fmt.Errorf("unmarshal fixed window config: %w", err)
		}
		return NewFixedWindowAlgorithm(fixedWindowConfig, batcher), nil
	default:
		return nil, fmt.Errorf("unknown algorithm: %s", name)
	}
}
