package main

type Command interface{}

const (
	// CommandSet is the SET command identifier
	CommandAllow = "ALLOW"
	// CommandBlock is the BLOCK command identifier
	CommandBlock = "BLOCK"
	// CommandCheck is the CHECK command identifier
	CommandCheck = "CHECK"
)

type AllowCommand struct {
	IP string
}

type BlockCommand struct {
	IP string
}

type CheckCommand struct {
	IP string
}
