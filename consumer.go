package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	Key             string
	BatchSize       int
	client          Adapter
	consumerSlot    chan struct{}
	wg              sync.WaitGroup
	topics          map[string][]HandleFunc
	cpuTime         float64
	StatListener    func(msg *StatMessage)
	pausePoll       atomic.Bool
	closed          bool
	circularBuffers map[string]*buffer
}
type HandleFunc func(c context.Context, body []byte, headers map[string]string) error

func NewConsumer(client Adapter, batchSize int, concurrency int) *Consumer {
	c := &Consumer{
		Key:             strconv.FormatUint(rand.Uint64(), 36),
		BatchSize:       batchSize,
		client:          client,
		consumerSlot:    make(chan struct{}, concurrency),
		wg:              sync.WaitGroup{},
		topics:          map[string][]HandleFunc{},
		cpuTime:         getCPUTimeSec(),
		circularBuffers: make(map[string]*buffer, 10),
	}
	return c

}
func (c *Consumer) AddWork(w *work) bool {
	key := fmt.Sprintf("%s|%d", w.r.Topic, w.r.Partition)
	if _, ok := c.circularBuffers[key]; !ok {
		c.circularBuffers[key] = &buffer{works: make([]*work, cap(c.consumerSlot)), capacity: cap(c.consumerSlot)}
	}
	return c.circularBuffers[key].Add(w)
}

// register listener and it gets invoked once message recived in topic
func (c *Consumer) Listen(topic string, fn HandleFunc) {
	if _, exists := c.topics[topic]; !exists {
		c.topics[topic] = []HandleFunc{}
	}
	c.client.ConsumeTopics([]string{topic})
	c.topics[topic] = append(c.topics[topic], fn)
}

func (c *Consumer) Run(ctx context.Context) {
	defer c.shutdown()
	go c.periodicCommit(ctx)

	for {

		if cap(c.consumerSlot)-len(c.consumerSlot) < c.BatchSize {
			c.SendStat("waiting for commits", "max_concurrent_jobs")
			fmt.Println("Waiting: max concurrancy is hit")
			time.Sleep(time.Millisecond * 100)
			continue
		}
		c.SendStat("waiting for next batch", "polling")
		if c.pausePoll.Load() == true {
			fmt.Println("Waiting: polling paused")
			c.SendStat("waiting for force commit completion", "polling_paused")

			time.Sleep(time.Millisecond * 100)
			continue
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
		for _, r := range records {
			if _, exists := c.topics[r.Topic]; !exists {
				// log if needed
				continue
			}
			select {
			case <-ctx.Done():
				return
			case c.consumerSlot <- struct{}{}:
			}
			c.SendStat("started consuming", "processing")

			v := r.Value
			r.Value = nil
			w := &work{r, 0}

			for !c.AddWork(w) {
				c.SendStat("add failed", "partition_buffer_full")
				fmt.Println("Partition buffer full — AddWork returned false")
				time.Sleep(time.Millisecond * 100)
				continue
			}
			c.wg.Add(1)
			go func(w *work, rec []byte, r *kgo.Record) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("panic during business logic: %v", r)
					}
					c.wg.Done()
					atomic.StoreUint32(&w.done, 1)
					<-c.consumerSlot
				}()

				for _, handler := range c.topics[r.Topic] {
					localCtx := context.WithValue(ctx, "messageId", fmt.Sprintf("%s:%d:%d", w.r.Topic, w.r.Partition, w.r.Offset))
					headers := make(map[string]string, len(r.Headers))
					for _, h := range r.Headers {
						headers[h.Key] = string(h.Value)
					}
					handler(localCtx, v, headers)
				}
			}(w, v, r)

		}

	}
}

func (c *Consumer) commit(ctx context.Context, reason string) (int, int) {
	var tc, tu int
	for _, b := range c.circularBuffers {
		c, u := b.Commit(ctx, c.client, reason)
		tc += c
		tu += u
	}
	return tc, tu
}

func (c *Consumer) totalBufferSize() int {
	var size int
	for _, b := range c.circularBuffers {
		size += b.size
	}
	return size
}

func (c *Consumer) shutdown() {
	fmt.Println("start of the shutdown")
	c.SendStat("shutdown signal triggerd", "preparing_close")
	c.wg.Wait()
	fmt.Println("all current event completed")
	c.SendStat("in progress message processing done", "preparing_close")

	finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.commit(finalCtx, "shutdown")
	c.closed = true
	c.SendStat("shudown completed", "closed")
	fmt.Println("shutdown completed")

}

type work struct {
	r    *kgo.Record
	done uint32
}
type StatMessage struct {
	Message     string                     `json:"message"`
	Event       string                     `json:"event"`           // Always "stat"
	Key         string                     `json:"key"`             // Unique consumer key
	Status      string                     `json:"status"`          // "processing", "busy", etc.
	ActiveJobs  int                        `json:"active_jobs"`     // Number of jobs in-progress
	MemoryMB    float64                    `json:"memory_mb"`       // Consumer memory usage
	CPUPercent  float64                    `json:"cpu_percent"`     // Consumer CPU usage
	Concurrency int                        `json:"max_concurrency"` // Total concurrency limit
	Buffers     map[string]*BufferSnapshot `json:"buffer"`
	PollSize    int                        `json:"poll_size"`
}
type BufferSnapshot struct {
	Buffer []*int `json:"buffer"`
	Head   int
	Tail   int
}
type buffer struct {
	mu       sync.Mutex
	works    []*work
	head     int
	tail     int
	size     int
	capacity int
}

func (b *buffer) FreeSpace() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.capacity - b.size
}
func (b *buffer) Add(w *work) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.size >= b.capacity {
		return false // no space left
	}
	b.works[b.tail] = w

	b.tail = (b.tail + 1) % b.capacity
	b.size++
	return true
}
func (b *buffer) Snapshot() []*int {
	b.mu.Lock()
	defer b.mu.Unlock()
	snapshot := make([]*int, b.capacity)
	for i, v := range b.works {
		var done *int
		if v != nil {
			d := 0
			if v.done == 1 {
				d = 1
			}
			done = &d
		}
		snapshot[i] = done
	}
	return snapshot
}

func (b *buffer) Commit(ctx context.Context, client Adapter, reason string) (int, int) {
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
		err := client.Commit(ctx, committed)
		fmt.Printf("%s committed %d out of %d records at %s and error is %v\n", reason, len(committed), b.size, time.Now(), err)
		if err == nil {
			oldHead := b.head
			for oldHead != head {
				b.works[oldHead] = nil
				oldHead = (oldHead + 1) % b.capacity
			}
			b.head = head
			b.size = size
		}
		return len(committed), b.size - len(committed)

	}
	return 0, b.size
}

func (c *Consumer) periodicCommit(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			commited, uncommited := c.commit(ctx, "periodic")
			if commited != 0 || uncommited != 0 {

				c.SendStat(fmt.Sprintf("commit done %d|%d", commited, uncommited), "")
			}
		}
	}
}
func (c *Consumer) ListenStats(fn func(msg *StatMessage)) {
	c.StatListener = fn
}
func (c *Consumer) SendStat(msg, status string) {
	if c.StatListener == nil {
		return
	}
	bs := make(map[string]*BufferSnapshot, len(c.circularBuffers))
	for k, b := range c.circularBuffers {
		bs[k] = &BufferSnapshot{
			Head:   b.head,
			Tail:   b.tail,
			Buffer: b.Snapshot(),
		}
	}
	s := &StatMessage{
		Message:     msg,
		Key:         c.Key,
		Event:       "stat",
		Status:      status,
		ActiveJobs:  len(c.consumerSlot),
		Concurrency: cap(c.consumerSlot),
		PollSize:    c.BatchSize,
		Buffers:     bs,
		MemoryMB:    getMemoryMB(),
		CPUPercent:  measureCPUPercent(c.cpuTime),
	}
	c.StatListener(s)
}
func getMemoryMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024 // in MB
}
func measureCPUPercent(start float64) float64 {

	end := getCPUTimeSec()

	numCPU := float64(runtime.NumCPU())
	return ((end - start) / 1.0) * 100 / numCPU
}

func getCPUTimeSec() float64 {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return -1
	}

	fields := strings.Fields(string(data))
	utime, uerr := strconv.ParseFloat(fields[13], 64)
	stime, serr := strconv.ParseFloat(fields[14], 64)
	if uerr != nil || serr != nil {
		return -1
	}
	return (utime + stime) / 100
}
func (c *Consumer) ForceCommit(ctx context.Context, reason string) {
	if c.closed {
		return
	}
	if c.totalBufferSize() == 0 && len(c.consumerSlot) == 0 {
		return
	}
	msg := reason + ": initiate force commit"
	c.SendStat(msg, "")
	fmt.Println("===> " + msg + " <===")
	// pause poll so current message can be commited
	c.pausePoll.Store(true)
	// commmit already completed work on safe side.
	c.commit(ctx, reason)

	// wait for pending work to complete.
	c.wg.Wait()
	// final commit
	c.commit(ctx, reason+":finailization")
	c.pausePoll.Store(false)
	msg = reason + ": force commit done"
	fmt.Println("===> " + msg + " <===")
	c.SendStat(msg, "")

}
