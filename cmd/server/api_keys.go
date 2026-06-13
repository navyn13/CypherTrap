package main

import (
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

func (s *Server) lookupAlgorithmAndConfig(companyName, keyName string) (Algorithm, error) {
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

func (s *Server) verifyAPIKey(companyName, keyName, apiKey string) bool {
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
	// bcrypt.CompareHashAndPassword expects []byte
	err = bcrypt.CompareHashAndPassword([]byte(apiHash), []byte(apiKey))
	return err == nil
}
