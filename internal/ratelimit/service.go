package ratelimit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const policyCacheTTL = time.Hour

type localCacheEntry struct {
	algo      Algorithm
	expiresAt time.Time
}

type Service struct {
	db         *pgxpool.Pool
	rdb        *redis.Client
	batcher    *CheckBatcher
	localCache sync.Map // map[string]localCacheEntry
}

func NewService(db *pgxpool.Pool, rdb *redis.Client) *Service {
	return &Service{
		db:      db,
		rdb:     rdb,
		batcher: NewCheckBatcher(rdb),
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

	if algo, ok := s.localCacheHit(cacheKey); ok {
		return algo, nil
	}

	cached, err := s.rdb.Get(ctx, cacheKey).Result()
	if err == nil {
		var policy policyEntry
		if err := json.Unmarshal([]byte(cached), &policy); err != nil {
			return nil, fmt.Errorf("unmarshal cached policy: %w", err)
		}
		algo, err := NewAlgorithmFromDB(policy.Algorithm, policy.Config, s.batcher)
		if err != nil {
			return nil, err
		}
		s.localCache.Store(cacheKey, localCacheEntry{algo: algo, expiresAt: time.Now().Add(policyCacheTTL)})
		return algo, nil
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("redis get policy: %w", err)
	}

	policy, err := s.lookupPolicyFromDB(ctx, companyName, keyName, policyName)
	if err != nil {
		return nil, err
	}

	if data, err := json.Marshal(policy); err == nil {
		s.rdb.Set(ctx, cacheKey, data, policyCacheTTL).Err()
	}

	algo, err := NewAlgorithmFromDB(policy.Algorithm, policy.Config, s.batcher)
	if err != nil {
		return nil, err
	}
	s.localCache.Store(cacheKey, localCacheEntry{algo: algo, expiresAt: time.Now().Add(policyCacheTTL)})
	return algo, nil
}

func (s *Service) localCacheHit(cacheKey string) (Algorithm, bool) {
	v, ok := s.localCache.Load(cacheKey)
	if !ok {
		return nil, false
	}
	entry := v.(localCacheEntry)
	if time.Now().After(entry.expiresAt) {
		s.localCache.Delete(cacheKey)
		return nil, false
	}
	return entry.algo, true
}

// InvalidatePolicy removes local policy entries for a company/key pair.
func (s *Service) InvalidatePolicy(company, keyName string) {
	prefix := fmt.Sprintf("policy_config:%s:%s:", company, keyName)
	s.localCache.Range(func(key, _ any) bool {
		if k, ok := key.(string); ok && strings.HasPrefix(k, prefix) {
			s.localCache.Delete(k)
		}
		return true
	})
}

// ClearPolicyCache drops all local policy entries.
func (s *Service) ClearPolicyCache() {
	s.localCache.Range(func(key, _ any) bool {
		s.localCache.Delete(key)
		return true
	})
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
