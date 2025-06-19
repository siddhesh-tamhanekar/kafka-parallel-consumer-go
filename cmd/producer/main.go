package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	fmt.Println("starting main....")
	ctx, cancel := context.WithCancel(context.Background())
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.DefaultProduceTopic("abc"),
	)
	defer client.Close()
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}

	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigterm
		fmt.Println("SIGTERM received, initiating shutdown...")
		cancel()
	}()
	i := 0
	// run the consumer
	for {

		select {
		case <-ctx.Done():
			fmt.Println("shutdown producer")
			return
		default:
			i++
			t, e := random()
			payload := map[string]any{
				"name": e,
				"data": i,
			}
			v, _ := json.Marshal(payload)
			fmt.Println(t, payload)
			r := kgo.Record{

				Topic: t,
				Value: v,
			}
			results := client.ProduceSync(ctx, &r)
			for _, res := range results {
				if res.Err != nil {
					log.Printf("Failed to produce to %s [partition %d]: %v", res.Record.Topic, res.Record.Partition, res.Err)
				} else {
					fmt.Printf("Message produced to %s [partition %d, offset %d]\n",
						res.Record.Topic, res.Record.Partition, res.Record.Offset)
				}
				time.Sleep(time.Second)
			}

		}
	}
}

func random() (string, string) {
	// Map of topic -> events
	topicEvents := map[string][]string{
		// "payments":          {"created", "failed", "retried"},
		"quickstart-events": {"created", "updated", "deleted"},
		// "orders":            {"placed", "cancelled", "shipped"},
	}
	rand.Seed(time.Now().UnixNano())

	topics := make([]string, 0, len(topicEvents))
	for topic := range topicEvents {
		topics = append(topics, topic)
	}
	randomTopic := topics[rand.Intn(len(topics))]

	events := topicEvents[randomTopic]
	return randomTopic, events[rand.Intn(len(events))]
}
