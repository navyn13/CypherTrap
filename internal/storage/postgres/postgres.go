package postgres

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

func NewDB(dbURL string) (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), dbURL)
	if err != nil {
		log.Println("Error connecting to database:", err)
		return nil, err
	}
	return conn, nil
}

func CloseDB(conn *pgx.Conn) {
	conn.Close(context.Background())
}
