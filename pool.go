package redowl

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type WorkerPool struct {
	client       redis.UniversalClient
	prefix       string
	minWorkers   int
	maxWorkers   int
	handler      func(context.Context, string, *Message) error
	queueIdle    time.Duration
	workerIdle   time.Duration
	pollInterval time.Duration
	runCtx       context.Context

	mu       sync.RWMutex
	queues   map[string]*queueState
	workCh   chan string
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
	pubsub   *redis.PubSub

	workersMu    sync.Mutex
	workersLive  int
	nextWorkerID int
}

type queueState struct {
	name       string
	lastActive time.Time
	msgCount   int64
	active     bool
	pending    bool
}

type PoolOption func(*WorkerPool)

func WithWorkerCount(n int) PoolOption {
	// Backward-compatible: treat as max worker count.
	return func(p *WorkerPool) { p.maxWorkers = n }
}

func WithMinWorkers(n int) PoolOption {
	return func(p *WorkerPool) { p.minWorkers = n }
}

func WithIdleTimeout(d time.Duration) PoolOption {
	// Backward-compatible: treat as per-queue idle timeout.
	return func(p *WorkerPool) { p.queueIdle = d }
}

// WithWorkerIdleTimeout controls how long a worker goroutine waits for new work
// before exiting (down to MinWorkers).
func WithWorkerIdleTimeout(d time.Duration) PoolOption {
	return func(p *WorkerPool) { p.workerIdle = d }
}

func WithPollInterval(d time.Duration) PoolOption {
	return func(p *WorkerPool) { p.pollInterval = d }
}

func NewWorkerPool(
	client redis.UniversalClient,
	prefix string,
	handler func(context.Context, string, *Message) error,
	opts ...PoolOption,
) *WorkerPool {
	p := &WorkerPool{
		client:       client,
		prefix:       prefix,
		minWorkers:   0,
		maxWorkers:   10,
		handler:      handler,
		queueIdle:    5 * time.Minute,
		workerIdle:   30 * time.Second,
		pollInterval: 500 * time.Millisecond,
		queues:       make(map[string]*queueState),
		workCh:       make(chan string, 1000),
		stopCh:       make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *WorkerPool) Start(ctx context.Context) error {
	p.runCtx = ctx
	p.pubsub = p.client.Subscribe(ctx, p.prefix+":events")
	if _, err := p.pubsub.Receive(ctx); err != nil {
		return err
	}

	// 启动事件监听器
	p.wg.Add(1)
	go p.eventListener(ctx)

	// 启动最小 worker 数
	if p.minWorkers < 0 {
		p.minWorkers = 0
	}
	if p.maxWorkers < 1 {
		p.maxWorkers = 1
	}
	if p.minWorkers > p.maxWorkers {
		p.minWorkers = p.maxWorkers
	}
	for i := 0; i < p.minWorkers; i++ {
		p.spawnWorker()
	}

	// 启动空闲队列清理器
	p.wg.Add(1)
	go p.idleCleaner(ctx)

	return nil
}

func (p *WorkerPool) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		if p.pubsub != nil {
			_ = p.pubsub.Close()
		}
	})
	p.wg.Wait()
}

func (p *WorkerPool) eventListener(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case msg, ok := <-p.pubsub.Channel():
			if !ok {
				return
			}

			var ev Event
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				continue
			}

			if ev.Type == EventSent && ev.Queue != "" {
				p.addQueue(ev.Queue)
			}
		}
	}
}

func (p *WorkerPool) addQueue(name string) {
	now := time.Now()
	shouldEnqueue := false

	p.mu.Lock()
	qs, exists := p.queues[name]
	if !exists {
		qs = &queueState{name: name, lastActive: now}
		p.queues[name] = qs
	}
	qs.lastActive = now
	if !qs.active {
		qs.active = true
		shouldEnqueue = true
	} else {
		qs.pending = true
	}
	p.mu.Unlock()

	if shouldEnqueue {
		select {
		case p.workCh <- name:
		default:
		}
		p.maybeSpawnWorker()
	}
}

func (p *WorkerPool) maybeSpawnWorker() {
	// Spawn at most one worker per call if we have pending work and have capacity.
	if len(p.workCh) == 0 {
		return
	}
	if p.workerIdle <= 0 {
		p.workerIdle = 30 * time.Second
	}

	p.workersMu.Lock()
	canSpawn := p.workersLive < p.maxWorkers
	p.workersMu.Unlock()
	if canSpawn {
		p.spawnWorker()
	}
}

func (p *WorkerPool) spawnWorker() {
	p.workersMu.Lock()
	if p.workersLive >= p.maxWorkers {
		p.workersMu.Unlock()
		return
	}
	id := p.nextWorkerID
	p.nextWorkerID++
	p.workersLive++
	p.workersMu.Unlock()

	p.wg.Add(1)
	ctx := p.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	go p.worker(ctx, id)
}

func (p *WorkerPool) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	defer func() {
		p.workersMu.Lock()
		p.workersLive--
		p.workersMu.Unlock()
	}()

	if p.workerIdle <= 0 {
		p.workerIdle = 30 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case queueName := <-p.workCh:
			p.processQueue(ctx, queueName)
			p.finishQueue(queueName)
		case <-time.After(p.workerIdle):
			p.workersMu.Lock()
			tooMany := p.workersLive > p.minWorkers
			p.workersMu.Unlock()
			if tooMany {
				return
			}
		}
	}
}

func (p *WorkerPool) finishQueue(queueName string) {
	// Mark queue inactive, and if new work arrived during processing, re-enqueue.
	shouldRequeue := false
	p.mu.Lock()
	if qs, ok := p.queues[queueName]; ok {
		if qs.pending {
			qs.pending = false
			shouldRequeue = true
		} else {
			qs.active = false
		}
		// If pending, keep active=true and re-enqueue.
		if shouldRequeue {
			qs.active = true
			qs.lastActive = time.Now()
		}
	}
	p.mu.Unlock()

	if shouldRequeue {
		select {
		case p.workCh <- queueName:
		default:
		}
		p.maybeSpawnWorker()
	}
}

func (p *WorkerPool) processQueue(ctx context.Context, queueName string) {
	q, err := New(p.client, queueName,
		WithPrefix(p.prefix),
		WithVisibilityTimeout(30*time.Second),
	)
	if err != nil {
		return
	}
	defer q.StopReaper()

	if p.queueIdle <= 0 {
		p.queueIdle = 5 * time.Minute
	}
	lastMsgAt := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		default:
		}

		msg, err := q.ReceiveWithWait(ctx, p.pollInterval)
		if err != nil {
			return
		}

		if msg == nil {
			if time.Since(lastMsgAt) >= p.queueIdle {
				return
			}
			continue
		}

		lastMsgAt = time.Now()
		p.updateQueueActivity(queueName)

		if err := p.handler(ctx, queueName, msg); err == nil {
			_ = q.Ack(ctx, msg.ReceiptHandle)
		}
	}
}

func (p *WorkerPool) updateQueueActivity(name string) {
	p.mu.Lock()
	if qs, ok := p.queues[name]; ok {
		qs.lastActive = time.Now()
		qs.msgCount++
	}
	p.mu.Unlock()
}

func (p *WorkerPool) idleCleaner(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.cleanIdleQueues()
		}
	}
}

func (p *WorkerPool) cleanIdleQueues() {
	now := time.Now()
	p.mu.Lock()
	for name, qs := range p.queues {
		if !qs.active && now.Sub(qs.lastActive) > p.queueIdle {
			delete(p.queues, name)
		}
	}
	p.mu.Unlock()
}

func (p *WorkerPool) Stats() map[string]int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := make(map[string]int64)
	for name, qs := range p.queues {
		stats[name] = qs.msgCount
	}
	return stats
}
