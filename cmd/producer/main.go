package main

import (
	"context"
	"encoding/json"
	"flag"
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
	var interval, messageCount int
	flag.IntVar(&interval, "i", 1000, "interval between two messages in milliseconds")
	flag.IntVar(&messageCount, "c", 10, "no of messages needs to be produced")
	flag.Parse()
	fmt.Println("starting main....")
	ctx, cancel := context.WithCancel(context.Background())
	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.DefaultProduceTopic("abc"),
	)
	if err != nil {
		log.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()

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
				Key:   []byte(fmt.Sprintf("key#%d", v)),
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
			}
			if i > messageCount {
				fmt.Println("Message produced: ", i)
				return
			}
			time.Sleep(time.Millisecond * time.Duration(interval))

		}
	}
}

func random() (string, string) {
	// Map of topic -> events
	topicEvents := map[string][]string{
		"test-topic": {"created", "failed", "retried"},
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
