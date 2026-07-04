package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
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

func (s *Service) VerifyAPIKey(companyName, keyName, apiKey string) bool {

	ctx := context.Background()

	// Create a fast hash of the incoming API key for cache lookup
	apiKeyHash := hashAPIKey(apiKey)
	cacheKey := fmt.Sprintf("api_verified:%s:%s:%s", companyName, keyName, apiKeyHash)

	// Check if this exact API key was verified before (Redis cache)
	cachedResult, err := s.rdb.Get(ctx, cacheKey).Result()
	if err == nil && cachedResult == "1" {
		return true
	}

	// Cache miss -> query DB and verify with bcrypt
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

	// Verify with bcrypt (slow, but only done once per unique API key)
	if bcrypt.CompareHashAndPassword([]byte(storedBcryptHash), []byte(apiKey)) != nil {
		return false
	}

	// Cache the successful verification result
	s.rdb.Set(ctx, cacheKey, "1", time.Hour).Err()

	return true
}

func hashAPIKey(apiKey string) string {
	hash := sha256.Sum256([]byte(apiKey))
	return hex.EncodeToString(hash[:])
}
