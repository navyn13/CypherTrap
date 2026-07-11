package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

const verifiedCacheTTL = time.Hour

type localCacheEntry struct {
	expiresAt time.Time
}

type Service struct {
	db         *pgxpool.Pool
	rdb        *redis.Client
	localCache sync.Map // map[string]localCacheEntry
}

func NewService(db *pgxpool.Pool, rdb *redis.Client) *Service {
	return &Service{
		db:  db,
		rdb: rdb,
	}
}

func (s *Service) VerifyAPIKey(companyName, keyName, apiKey string) bool {
	ctx := context.Background()

	apiKeyHash := hashAPIKey(apiKey)
	cacheKey := fmt.Sprintf("api_verified:%s:%s:%s", companyName, keyName, apiKeyHash)

	if s.localCacheHit(cacheKey) {
		return true
	}

	cachedResult, err := s.rdb.Get(ctx, cacheKey).Result()
	if err == nil && cachedResult == "1" {
		s.localCache.Store(cacheKey, localCacheEntry{expiresAt: time.Now().Add(verifiedCacheTTL)})
		return true
	}

	var storedBcryptHash string
	err = s.db.QueryRow(
		ctx,
		`SELECT ak.key_hash
		 FROM api_keys ak
		 JOIN companies c ON c.id = ak.company_id
		 WHERE c.name = $1
		   AND ak.name = $2
		   AND ak.is_active = TRUE`,
		companyName,
		keyName,
	).Scan(&storedBcryptHash)
	if err != nil {
		return false
	}

	if bcrypt.CompareHashAndPassword([]byte(storedBcryptHash), []byte(apiKey)) != nil {
		return false
	}

	s.rdb.Set(ctx, cacheKey, "1", verifiedCacheTTL).Err()
	s.localCache.Store(cacheKey, localCacheEntry{expiresAt: time.Now().Add(verifiedCacheTTL)})
	return true
}

func (s *Service) localCacheHit(cacheKey string) bool {
	v, ok := s.localCache.Load(cacheKey)
	if !ok {
		return false
	}
	entry := v.(localCacheEntry)
	if time.Now().After(entry.expiresAt) {
		s.localCache.Delete(cacheKey)
		return false
	}
	return true
}

// InvalidateAPIKey removes local verification entries for a company/key pair.
func (s *Service) InvalidateAPIKey(company, keyName string) {
	prefix := fmt.Sprintf("api_verified:%s:%s:", company, keyName)
	s.localCache.Range(func(key, _ any) bool {
		if k, ok := key.(string); ok && strings.HasPrefix(k, prefix) {
			s.localCache.Delete(k)
		}
		return true
	})
}

// ClearVerifiedCache drops all local verification entries.
func (s *Service) ClearVerifiedCache() {
	s.localCache.Range(func(key, _ any) bool {
		s.localCache.Delete(key)
		return true
	})
}

func hashAPIKey(apiKey string) string {
	hash := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(hash[:])
}
