package auth

import (
	"context"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
)

type Service struct {
	db *pgx.Conn
}

func NewService(db *pgx.Conn) *Service {
	return &Service{
		db: db,
	}
}

func (s *Service) VerifyAPIKey(companyName, keyName, apiKey string) bool {
	var apiHash string
	err := s.db.QueryRow(
		context.Background(),
		`SELECT ak.key_hash FROM api_keys ak
JOIN companies c ON c.id = ak.company_id
WHERE c.name = $1
  AND ak.name = $2
  AND ak.is_active = TRUE`,
		companyName,
		keyName,
	).Scan(&apiHash)
	if err != nil {
		return false
	}
	err = bcrypt.CompareHashAndPassword([]byte(apiHash), []byte(apiKey))
	return err == nil
}
