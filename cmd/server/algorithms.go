package main

import (
	"encoding/json"
	"fmt"
)

type Algorithm interface {
	Check() bool
}
type fixedWindowAlgorithm struct {
	config fixedWindowConfig
}

type fixedWindowConfig struct {
	WindowSize int `json:"window_size"`
	WindowMs   int `json:"window_ms"`
}

func NewFixedWindowAlgorithm(config fixedWindowConfig) Algorithm {
	return &fixedWindowAlgorithm{
		config: config,
	}
}
func (a *fixedWindowAlgorithm) Check() bool {
	windowSize := a.config.WindowSize
	windowMs := a.config.WindowMs
	fmt.Println("windowSize:", windowSize)
	fmt.Println("windowMs:", windowMs)
	return true
}

func NewAlgorithmFromDB(name string, config json.RawMessage) (Algorithm, error) {
	switch name {
	case "fixed_window":
		var fixedWindowConfig fixedWindowConfig
		if err := json.Unmarshal(config, &fixedWindowConfig); err != nil {
			return nil, fmt.Errorf("unmarshal fixed window config: %w", err)
		}
		return NewFixedWindowAlgorithm(fixedWindowConfig), nil
	default:
		return nil, fmt.Errorf("unknown algorithm: %s", name)
	}
}
