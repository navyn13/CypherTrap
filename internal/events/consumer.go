package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type APIKeyEvent struct {
	APIKeyID  string    `json:"apiKeyId"`
	Event     string    `json:"event"`
	Timestamp time.Time `json:"timestamp"`
	Company   string    `json:"company,omitempty"`
	KeyName   string    `json:"keyName,omitempty"`
}

type Consumer struct {
	consumer sarama.ConsumerGroup
	rdb      *redis.Client
	db       *pgxpool.Pool
	topics   []string
	ready    chan bool
}

func NewConsumer(brokers []string, groupID string, topics []string, rdb *redis.Client, db *pgxpool.Pool) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_6_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, fmt.Errorf("create consumer group: %w", err)
	}

	return &Consumer{
		consumer: consumer,
		rdb:      rdb,
		db:       db,
		topics:   topics,
		ready:    make(chan bool),
	}, nil
}

func (c *Consumer) Start(ctx context.Context) error {
	handler := &consumerGroupHandler{
		rdb:   c.rdb,
		db:    c.db,
		ready: c.ready,
	}

	go func() {
		for {
			if err := c.consumer.Consume(ctx, c.topics, handler); err != nil {
				slog.Error("Consumer error", "err", err)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-c.ready
	slog.Info("Kafka consumer ready", "topics", c.topics)
	return nil
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}

type consumerGroupHandler struct {
	rdb   *redis.Client
	db    *pgxpool.Pool
	ready chan bool
}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if err := h.handleMessage(message); err != nil {
			slog.Error("Failed to handle message", "err", err, "offset", message.Offset)
		}
		session.MarkMessage(message, "")
	}
	return nil
}

func (h *consumerGroupHandler) handleMessage(msg *sarama.ConsumerMessage) error {
	var event APIKeyEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	slog.Info("Processing event", "apiKeyId", event.APIKeyID, "event", event.Event, "timestamp", event.Timestamp)

	switch event.Event {
	case "deleted", "revoked":
		return h.handleAPIKeyDeleted(event)
	default:
		slog.Warn("Unknown event type", "event", event.Event)
	}

	return nil
}

func (h *consumerGroupHandler) handleAPIKeyDeleted(event APIKeyEvent) error {
	ctx := context.Background()
	company, keyName := event.Company, event.KeyName

	if company == "" || keyName == "" {
		err := h.db.QueryRow(
			ctx,
			`SELECT c.name, ak.name
			 FROM api_keys ak
			 JOIN companies c ON c.id = ak.company_id
			 WHERE ak.id = $1`,
			event.APIKeyID,
		).Scan(&company, &keyName)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				slog.Warn("API key already removed from DB, clearing all verification cache",
					"apiKeyId", event.APIKeyID)
				return h.clearAllAPIKeyCache()
			}
			return fmt.Errorf("lookup api key %s: %w", event.APIKeyID, err)
		}
	}

	deletedCount, err := h.invalidateAPIKeyCache(ctx, company, keyName)
	if err != nil {
		return err
	}

	slog.Info("API key verification cache invalidated",
		"apiKeyId", event.APIKeyID,
		"company", company,
		"keyName", keyName,
		"deletedKeys", deletedCount)

	return nil
}

func (h *consumerGroupHandler) invalidateAPIKeyCache(ctx context.Context, company, keyName string) (int, error) {
	pattern := fmt.Sprintf("api_verified:%s:%s:*", company, keyName)

	iter := h.rdb.Scan(ctx, 0, pattern, 100).Iterator()
	deletedCount := 0

	for iter.Next(ctx) {
		if err := h.rdb.Del(ctx, iter.Val()).Err(); err != nil {
			slog.Error("Failed to delete key", "key", iter.Val(), "err", err)
		} else {
			deletedCount++
		}
	}

	if err := iter.Err(); err != nil {
		return deletedCount, fmt.Errorf("scan keys: %w", err)
	}

	return deletedCount, nil
}

func (h *consumerGroupHandler) clearAllAPIKeyCache() error {
	ctx := context.Background()
	pattern := "api_verified:*"

	iter := h.rdb.Scan(ctx, 0, pattern, 1000).Iterator()
	deletedCount := 0

	for iter.Next(ctx) {
		if err := h.rdb.Del(ctx, iter.Val()).Err(); err != nil {
			slog.Error("Failed to delete key", "key", iter.Val(), "err", err)
		} else {
			deletedCount++
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("scan keys: %w", err)
	}

	slog.Warn("Cleared all API key cache entries", "deletedKeys", deletedCount)
	return nil
}
