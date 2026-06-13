package server

import (
	"fmt"
	"net"
)

type Message struct {
	Peer *Peer
	Msg  string
}

type Peer struct {
	conn  net.Conn
	msgCh chan Message
}

func NewPeer(conn net.Conn, msgCh chan Message) *Peer {
	return &Peer{
		conn:  conn,
		msgCh: msgCh,
	}
}

func (p *Peer) Send(msg []byte) (int, error) {
	return p.conn.Write(msg)
}

func (p *Peer) readLoop() error {
	buf := make([]byte, 1024)
	for {
		n, err := p.conn.Read(buf)
		if err != nil {
			fmt.Println("read error:", err)
			return err
		}
		if n == 0 {
			continue
		}
		p.msgCh <- Message{Peer: p, Msg: string(buf[:n])}
	}
}
