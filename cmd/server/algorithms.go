package main

type Algorithm interface {
	Check(ip string) bool
}
type fixedWindowAlgorithm struct {
	windowSize int
	window     map[string]int
}

func NewFixedWindowAlgorithm(windowSize int) Algorithm {
	return &fixedWindowAlgorithm{
		windowSize: windowSize,
		window:     make(map[string]int),
	}
}
func (a *fixedWindowAlgorithm) Check(ip string) bool {
	if a.window[ip] >= a.windowSize {
		return false
	}

	a.window[ip]++
	return true
}
