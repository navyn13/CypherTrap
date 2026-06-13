package main

type Algorithm interface {
	Check(ip string) bool
}
type fixedWindowAlgorithm struct {
	windowSize int
}

func NewFixedWindowAlgorithm(windowSize int) Algorithm {
	return &fixedWindowAlgorithm{
		windowSize: windowSize,
	}
}
func (a *fixedWindowAlgorithm) Check(ip string) bool {
	return true
}
