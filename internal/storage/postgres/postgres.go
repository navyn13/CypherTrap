package postgres

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewDB(dbURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Println("Error connecting to database:", err)
		return nil, err
	}
	return pool, nil
}

func CloseDB(pool *pgxpool.Pool) {
	pool.Close()
}
