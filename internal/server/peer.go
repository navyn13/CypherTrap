package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

type Message struct {
	Peer *Peer
	Msg  string
}

type Peer struct {
	conn  net.Conn
	msgCh chan Message
	mu    sync.Mutex // serializes writes (pipelined msgs may run on many workers)
}

func NewPeer(conn net.Conn, msgCh chan Message) *Peer {
	return &Peer{
		conn:  conn,
		msgCh: msgCh,
	}
}

func (p *Peer) Send(msg []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn.Write(msg)
}

func (p *Peer) readLoop() error {
	reader := bufio.NewReaderSize(p.conn, 64*1024)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("read error:", err)
			return err
		}
		msg := strings.TrimRight(line, "\r\n")
		if msg == "" {
			continue
		}
		p.msgCh <- Message{Peer: p, Msg: msg}
	}
}
