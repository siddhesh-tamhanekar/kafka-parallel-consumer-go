package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	consumer "github.com/your-username/kafka-parallel-consumer-go"
)

const BATCH_SIZE = 5           // kafka poll size
const MAX_CONCURRENT_JOBS = 20 // max conurrency suppported

var cons *consumer.Consumer
var client = &http.Client{}

func ForceCommit(ctx context.Context, cl *kgo.Client, m map[string][]int32) {
	cons.ForceCommit(ctx, "rebalance")
}
func main() {
	fmt.Printf("starting consume [PID: %d] ....", os.Getpid())
	ctx, cancel := context.WithCancel(context.Background())
	c, err := consumer.NewFranzGoAdapter([]string{"localhost:9092"}, kgo.DisableAutoCommit(), kgo.ConsumerGroup("test"),
		kgo.OnPartitionsRevoked(ForceCommit),
	)
	if err != nil {
		log.Fatalf("failed to create kafka adapter: %v", err)
	}
	defer c.Close()

	var sigterm = make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	cons = consumer.NewConsumer(c, BATCH_SIZE, MAX_CONCURRENT_JOBS)

	go func() {
		<-sigterm
		fmt.Println("SIGTERM received, initiating shutdown...")
		cancel()
	}()
	// run the consumer
	f, err := os.OpenFile("./outputs/"+cons.Key+".txt", os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		log.Fatalln("file open err", err)
	}
	defer f.Close()

	cons.Listen("test-topic", func(c context.Context, body []byte, headers map[string]string) error {
		var v struct {
			Data int    `json:"data"`
			Name string `json:"name"`
		}
		json.Unmarshal(body, &v)
		waitTime := v.Data % 10
		time.Sleep(time.Second * time.Duration(waitTime))
		_, err := fmt.Fprintln(f, v.Data)
		fmt.Println("file write error", err)
		return nil
	})
	cons.Listen("quickstart-events", func(c context.Context, body []byte, headers map[string]string) error {
		var v struct {
			Data int    `json:"data"`
			Name string `json:"name"`
		}
		json.Unmarshal(body, &v)
		waitTime := v.Data % 10
		fmt.Println("start message", string(body))
		time.Sleep(time.Second * time.Duration(waitTime))
		fmt.Println("end message", string(body))

		return nil
	})

	cons.ListenStats(func(msg *consumer.StatMessage) {
		b, err := json.Marshal(msg)
		if err != nil {
			log.Println("json unmarshal err", err)
		}
		br := bytes.NewBuffer(b)

		req, err := http.NewRequest("POST", "http://localhost:8081/broadcast", br)
		if err != nil {
			log.Println("request create err", err)
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Println("request send err", err)
			return
		}
		defer resp.Body.Close()
	})

	cons.Run(ctx)

}
