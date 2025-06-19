package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
	consumer "github.com/your-username/kafka-parallel-consumer-go"
)

const BATCH_SIZE = 5
const MAX_CONCURRENT_JOBS = 20

var cons *consumer.Consumer

func ForceCommit(ctx context.Context, cl *kgo.Client, m map[string][]int32) {
	fmt.Println("consumer")
	cons.Commit(ctx)
}
func main() {
	fmt.Println("starting main....")
	ctx, cancel := context.WithCancel(context.Background())
	c, err := consumer.NewFranzGoAdapter([]string{"localhost:9092"}, kgo.DisableAutoCommit(), kgo.ConsumerGroup("test"),
		kgo.OnPartitionsRevoked(ForceCommit),
	)
	defer c.Close()
	if err != nil {
		log.Fatalf("failed to create kafka adapter: %v", err)
	}

	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	cons = consumer.NewConsumer(c, []string{"quickstart-events"}, BATCH_SIZE, MAX_CONCURRENT_JOBS)

	go func() {
		<-sigterm
		fmt.Println("SIGTERM received, initiating shutdown...")
		cancel()
	}()
	// run the consumer
	cons.Run(ctx)

}
