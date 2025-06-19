package consumer

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

type FranzGoAdapter struct {
	client *kgo.Client
}

// NewAdapter initializes the Kafka client with the given brokers and options.
func NewFranzGoAdapter(brokers []string, opts ...kgo.Opt) (*FranzGoAdapter, error) {
	opts = append(opts, kgo.SeedBrokers(brokers...))

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &FranzGoAdapter{client: client}, nil
}

// Produce sends a single Kafka record.
func (a *FranzGoAdapter) Produce(ctx context.Context, record *kgo.Record) error {
	return a.client.ProduceSync(ctx, record).FirstErr()

}

// ConsumeRecords consumes up to n records with a timeout.
func (a *FranzGoAdapter) ConsumeRecords(ctx context.Context, n int) ([]*kgo.Record, error) {
	fetches := a.client.PollRecords(ctx, n)
	var records []*kgo.Record
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		for _, r := range p.Records {
			records = append(records, r)
			if len(records) >= n {
				return
			}
		}
	})
	if fetches.IsClientClosed() {
		return nil, errors.New("client closed")
	}
	return records, fetches.Err()
}

func (a *FranzGoAdapter) ConsumeTopics(topics []string) {
	a.client.AddConsumeTopics(topics...)

}

// Commit commits the provided records' offsets.
func (a *FranzGoAdapter) Commit(ctx context.Context, records []*kgo.Record) error {
	return a.client.CommitRecords(ctx, records...)
}

// Close closes the Kafka client.
func (a *FranzGoAdapter) Close() error {
	a.client.Close()
	return nil
}
