package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	db  *pgx.Conn
	rdb *redis.Client
}

func NewService(db *pgx.Conn, rdb *redis.Client) *Service {
	return &Service{
		db:  db,
		rdb: rdb,
	}
}

func (s *Service) LookupAlgorithmAndConfig(companyName, keyName string) (Algorithm, error) {
	var algorithmName string
	var algorithmConfig json.RawMessage
	err := s.db.QueryRow(
		context.Background(),
		`SELECT ak.algorithm, ak.config
FROM api_keys ak
JOIN companies c ON c.id = ak.company_id
WHERE c.name = $1
  AND ak.name = $2
  AND ak.is_active = TRUE`,
		companyName,
		keyName,
	).Scan(&algorithmName, &algorithmConfig)
	if err != nil {
		return nil, fmt.Errorf("lookup api key: %w", err)
	}

	return NewAlgorithmFromDB(algorithmName, algorithmConfig, s.rdb)
}
