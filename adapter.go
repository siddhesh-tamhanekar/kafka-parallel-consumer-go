package consumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Adapter interface {
	// Produce sends a single Kafka record synchronously.
	Produce(ctx context.Context, record *kgo.Record) error

	ConsumeTopics(topics []string)

	// ConsumeRecords fetches up to 'n' Kafka records.
	ConsumeRecords(ctx context.Context, n int) ([]*kgo.Record, error)

	// Commit commits the offsets for the provided Kafka records.
	Commit(ctx context.Context, records []*kgo.Record) error
	// Commit commits the offsets for the provided Kafka records.
	// ForceCommit(ctx context.Context) error

	// Close shuts down the Kafka client and cleans up resources.
	Close() error
}
