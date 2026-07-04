package ratelimit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	db  *pgxpool.Pool
	rdb *redis.Client
}

func NewService(db *pgxpool.Pool, rdb *redis.Client) *Service {
	return &Service{
		db:  db,
		rdb: rdb,
	}
}

type policyEntry struct {
	Name      string          `json:"name"`
	Algorithm string          `json:"algorithm"`
	Config    json.RawMessage `json:"config"`
}

func (s *Service) LookupAlgorithmAndConfig(companyName, keyName, policyName string) (Algorithm, error) {
	var policiesJSON json.RawMessage
	err := s.db.QueryRow(
		context.Background(),
		`SELECT ak.policies
FROM api_keys ak
JOIN companies c ON c.id = ak.company_id
WHERE c.name = $1
  AND ak.name = $2
  AND ak.is_active = TRUE`,
		companyName,
		keyName,
	).Scan(&policiesJSON)
	if err != nil {
		return nil, fmt.Errorf("lookup api key: %w", err)
	}

	var policies []policyEntry
	if err := json.Unmarshal(policiesJSON, &policies); err != nil {
		return nil, fmt.Errorf("unmarshal policies: %w", err)
	}

	for _, policy := range policies {
		if policy.Name == policyName {
			return NewAlgorithmFromDB(policy.Algorithm, policy.Config, s.rdb)
		}
	}

	return nil, fmt.Errorf("policy not found: %s", policyName)
}
