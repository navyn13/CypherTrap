package main

import (
	"fmt"
	"net"
)

type Peer struct {
	conn   net.Conn
	isAuth bool
}

func NewPeer(conn net.Conn) *Peer {
	return &Peer{
		conn: conn,
	}
}
func (p *Peer) Send(msg []byte) (int, error) {
	return p.conn.Write(msg)
}

func (p *Peer) readLoop() error {
	buf := make([]byte, 1024)

	n, _ := p.conn.Read(buf)

	fmt.Printf("%q\n", string(buf[:n]))
	return nil
}
