package ratelimit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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

func policyCacheKey(companyName, keyName, policyName string) string {
	return fmt.Sprintf("policy_config:%s:%s:%s", companyName, keyName, policyName)
}

func (s *Service) LookupAlgorithmAndConfig(companyName, keyName, policyName string) (Algorithm, error) {
	ctx := context.Background()
	cacheKey := policyCacheKey(companyName, keyName, policyName)

	cached, err := s.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var policy policyEntry
		if err := json.Unmarshal([]byte(cached), &policy); err != nil {
			return nil, fmt.Errorf("unmarshal cached policy: %w", err)
		}
		return NewAlgorithmFromDB(policy.Algorithm, policy.Config, s.rdb)
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("redis get policy: %w", err)
	}

	policy, err := s.lookupPolicyFromDB(ctx, companyName, keyName, policyName)
	if err != nil {
		return nil, err
	}

	if data, err := json.Marshal(policy); err == nil {
		s.rdb.Set(ctx, cacheKey, data, time.Hour).Err()
	}

	return NewAlgorithmFromDB(policy.Algorithm, policy.Config, s.rdb)
}

func (s *Service) lookupPolicyFromDB(ctx context.Context, companyName, keyName, policyName string) (policyEntry, error) {
	var policiesJSON json.RawMessage
	err := s.db.QueryRow(
		ctx,
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
		return policyEntry{}, fmt.Errorf("lookup api key: %w", err)
	}

	var policies []policyEntry
	if err := json.Unmarshal(policiesJSON, &policies); err != nil {
		return policyEntry{}, fmt.Errorf("unmarshal policies: %w", err)
	}

	for _, policy := range policies {
		if policy.Name == policyName {
			return policy, nil
		}
	}

	return policyEntry{}, fmt.Errorf("policy not found: %s", policyName)
}
