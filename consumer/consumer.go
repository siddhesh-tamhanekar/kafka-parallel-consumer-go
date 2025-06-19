package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	BatchSize      int
	client         Adapter
	circularBuffer buffer
	ch             chan struct{}
	wg             sync.WaitGroup
}

func NewConsumer(client Adapter, topics []string, batchSize int, concurrency int) *Consumer {
	c := &Consumer{
		BatchSize:      batchSize,
		client:         client,
		circularBuffer: buffer{works: make([]*work, concurrency*2), capacity: concurrency * 2},
		ch:             make(chan struct{}, concurrency),
		wg:             sync.WaitGroup{},
	}
	c.circularBuffer.client = c.client
	c.client.ConsumeTopics(topics)
	return c

}

func (c *Consumer) Run(ctx context.Context) {
	defer c.shutdown()
	go c.commit(ctx)

	for {

		for c.circularBuffer.FreeSpace() < c.BatchSize {
			fmt.Println("Waiting: not enough buffer space for batch")
			time.Sleep(time.Millisecond * 100)
		}
		records, err := c.client.ConsumeRecords(ctx, c.BatchSize)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				fmt.Println("Context canceled, stopping consumer loop")

				break
			}
			fmt.Println("kafka read error lets pause for some time", err)
			time.Sleep(time.Second)
			continue
		}
		workid := 0
		for _, r := range records {
			fmt.Printf("currnet concurrent jobs %d\n", len(c.ch))
			select {
			case <-ctx.Done():
				return
			case c.ch <- struct{}{}:
			}
			v := r.Value
			r.Value = nil
			w := &work{r, 0}
			c.circularBuffer.Add(w)
			c.wg.Add(1)
			go func(w *work, rec []byte, workid int) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("panic during business logic: %v", r)
					}
					c.wg.Done()
					atomic.StoreUint32(&w.done, 1)
					<-c.ch
				}()
				dur := time.Second * 1
				if workid%2 == 0 {
					dur = time.Second * 2
				}
				fmt.Printf("BUSINESS LOGIC got record started at %s: %d: %s\n", time.Now(), workid, string(rec))
				time.Sleep(dur)
				fmt.Printf("BUSINESS LOGIC got record completed at %s: %d: %s\n", time.Now(), workid, string(rec))
			}(w, v, workid)
			workid++
		}

	}
}

func (c *Consumer) shutdown() {
	fmt.Println("start of the shutdown")
	c.wg.Wait()
	fmt.Println("all current event completed")
	finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.circularBuffer.Commit(finalCtx)

	fmt.Println("all commits are done")
}

type work struct {
	r    *kgo.Record
	done uint32
}

type buffer struct {
	mu       sync.Mutex
	works    []*work
	head     int
	tail     int
	size     int
	capacity int
	client   Adapter
}

func (b *buffer) FreeSpace() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.capacity - b.size
}
func (b *buffer) Add(w *work) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.works[b.tail] = w

	b.tail = (b.tail + 1) % b.capacity
	b.size++

}

func (b *buffer) Commit(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var committed []*kgo.Record
	head := b.head
	size := b.size
	for size > 0 {
		w := b.works[head]
		if w == nil {
			break
		}
		if atomic.LoadUint32(&w.done) == 0 {
			break
		}

		committed = append(committed, w.r)
		head = (head + 1) % b.capacity
		size--
	}
	if len(committed) > 0 {
		err := b.client.Commit(ctx, committed)
		fmt.Printf("committed %d out of %d records at %s and error is %v\n", len(committed), b.size, time.Now(), err)
		if err == nil {
			oldHead := b.head
			for oldHead != head {
				b.works[oldHead] = nil
				oldHead = (oldHead + 1) % b.capacity
			}
			b.head = head
			b.size = size
		}
		b.Visualize()

	}

}
func (b *buffer) Visualize() {

	fmt.Printf("\n[BUFFER STATE at %s] head=%d tail=%d size=%d\n", time.Now().Format("15:04:05"), b.head, b.tail, b.size)

	for i := 0; i < b.capacity; i++ {
		w := b.works[i]
		var status string
		switch {
		case w == nil:
			status = "empty"
		case atomic.LoadUint32(&w.done) == 1:
			status = "done"
		default:
			status = "pending"
		}

		mark := " "
		if i == b.head {
			mark = "H"
		}
		if i == b.tail {
			mark += "T"
		}

		fmt.Printf("[%2d]%s %-7s ", i, mark, status)
		if (i+1)%8 == 0 { // break line for readability
			fmt.Println()
		}
	}
	fmt.Println()
}

func (c *Consumer) commit(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.Commit(ctx)
		}
	}
}
func (c *Consumer) Commit(ctx context.Context) {
	c.circularBuffer.Commit(ctx)
}
