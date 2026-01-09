//go:build rediscluster

package redowl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

const producerDoneEventType EventType = "producer_done"

func redisClusterAddrsFromEnv(t *testing.T) []string {
	t.Helper()

	addrsEnv := strings.TrimSpace(os.Getenv("REDIS_CLUSTER_ADDRS"))
	if addrsEnv == "" {
		t.Skip("set REDIS_CLUSTER_ADDRS (comma-separated) to run redis cluster integration tests")
	}
	addrs := strings.Split(addrsEnv, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	return addrs
}

func newClusterClients(t *testing.T, addrs []string) (*redis.ClusterClient, *redis.ClusterClient) {
	t.Helper()

	producerClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	consumerClient := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() {
		_ = producerClient.Close()
		_ = consumerClient.Close()
	})
	return producerClient, consumerClient
}

func newIntegrationPrefix() string {
	return fmt.Sprintf("redowl_it_%d", time.Now().UnixNano())
}

type dynamicConsumer struct {
	gotMu    sync.Mutex
	gotByKey map[string][]string

	inflight    int64
	maxInflight int64
	doneFlag    int32

	startedMu sync.Mutex
	started   map[string]bool

	pubsub      *redis.PubSub
	doneCh      chan struct{}
	starterDone chan struct{}
	wg          sync.WaitGroup
}

func (c *dynamicConsumer) closePubSub() {
	if c.pubsub != nil {
		_ = c.pubsub.Close()
	}
}

func (c *dynamicConsumer) waitDoneOrFail(ctx context.Context, t *testing.T) {
	t.Helper()
	select {
	case <-c.doneCh:
		return
	case <-ctx.Done():
		t.Fatal("timeout waiting for producer_done trigger")
	}
}

func (c *dynamicConsumer) waitStarterOrFail(ctx context.Context, t *testing.T) {
	t.Helper()
	select {
	case <-c.starterDone:
		return
	case <-ctx.Done():
		t.Fatal("timeout waiting for starter to drain")
	}
}

func startDynamicConsumer(
	ctx context.Context,
	t *testing.T,
	consumerClient *redis.ClusterClient,
	prefix string,
	isInterestingQueue func(string) bool,
) *dynamicConsumer {
	t.Helper()

	// NOTE: We intentionally do NOT use PSubscribe here.
	pubsub := consumerClient.Subscribe(ctx, prefix+":events")
	// Ensure subscription is active before producer starts publishing.
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	c := &dynamicConsumer{
		gotByKey:    map[string][]string{},
		started:     map[string]bool{},
		pubsub:      pubsub,
		doneCh:      make(chan struct{}),
		starterDone: make(chan struct{}),
	}

	processDelay := 30 * time.Millisecond
	idlePoll := 500 * time.Millisecond
	idleExitStreak := 6

	updateMax := func(v int64) {
		for {
			cur := atomic.LoadInt64(&c.maxInflight)
			if v <= cur {
				return
			}
			if atomic.CompareAndSwapInt64(&c.maxInflight, cur, v) {
				return
			}
		}
	}

	startWorker := func(queueName string) {
		c.startedMu.Lock()
		if c.started[queueName] {
			c.startedMu.Unlock()
			return
		}
		c.started[queueName] = true
		c.startedMu.Unlock()

		q, err := New(consumerClient, queueName, WithPrefix(prefix), WithVisibilityTimeout(5*time.Second))
		require.NoError(t, err)

		c.wg.Add(1)
		go func(q *Queue) {
			defer c.wg.Done()
			idleStreak := 0
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				msg, err := q.ReceiveWithWait(ctx, idlePoll)
				require.NoError(t, err)
				if msg == nil {
					if atomic.LoadInt32(&c.doneFlag) == 1 {
						idleStreak++
						if idleStreak >= idleExitStreak {
							return
						}
					}
					continue
				}
				idleStreak = 0

				done := make(chan struct{})
				go func(m *Message) {
					cur := atomic.AddInt64(&c.inflight, 1)
					updateMax(cur)
					defer atomic.AddInt64(&c.inflight, -1)

					time.Sleep(processDelay)

					c.gotMu.Lock()
					c.gotByKey[q.Name()] = append(c.gotByKey[q.Name()], string(m.Body))
					c.gotMu.Unlock()

					require.NoError(t, q.Ack(ctx, m.ReceiptHandle))
					close(done)
				}(msg)

				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("timeout waiting for message handler")
				}
			}
		}(q)
	}

	startCh := make(chan string, 64)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-pubsub.Channel():
				if !ok {
					return
				}
				var ev Event
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
					continue
				}
				if ev.Type == producerDoneEventType {
					atomic.StoreInt32(&c.doneFlag, 1)
					close(c.doneCh)
					return
				}
				if ev.Queue == "" {
					continue
				}
				if isInterestingQueue != nil && !isInterestingQueue(ev.Queue) {
					continue
				}
				if ev.Type == EventSent {
					select {
					case startCh <- ev.Queue:
					default:
					}
				}
			}
		}
	}()

	go func() {
		defer close(c.starterDone)
		for {
			select {
			case <-ctx.Done():
				return
			case qn := <-startCh:
				startWorker(qn)
			case <-c.doneCh:
				// Drain any pending queue starts then stop starting new workers.
				for {
					select {
					case qn := <-startCh:
						startWorker(qn)
					default:
						return
					}
				}
			}
		}
	}()

	return c
}

func runProducer(
	ctx context.Context,
	t *testing.T,
	producerClient *redis.ClusterClient,
	prefix string,
	gameCount int,
	perGameMsgs int,
) {
	t.Helper()

	producerQueues := make([]*Queue, 0, gameCount)
	for i := 0; i < gameCount; i++ {
		qName := fmt.Sprintf("Game%d", i+1)
		pq, err := New(producerClient, qName, WithPrefix(prefix), WithVisibilityTimeout(5*time.Second))
		require.NoError(t, err)
		producerQueues = append(producerQueues, pq)
	}
	for i := 0; i < gameCount; i++ {
		prefixChar := byte('A' + i)
		for j := 1; j <= perGameMsgs; j++ {
			payload := fmt.Sprintf("%c%d", prefixChar, j)
			_, err := producerQueues[i].Send(ctx, []byte(payload), map[string]string{"game": strconv.Itoa(i + 1)})
			require.NoError(t, err)
		}
	}
}

func publishProducerDone(ctx context.Context, t *testing.T, producerClient *redis.ClusterClient, prefix string) {
	t.Helper()
	b, _ := json.Marshal(Event{Type: producerDoneEventType, AtUnixMs: time.Now().UnixMilli(), Extra: map[string]string{"prefix": prefix}})
	require.NoError(t, producerClient.Publish(ctx, prefix+":events", b).Err())
}

func assertOrderedPerGame(t *testing.T, gotByKey map[string][]string, gameCount int, perGameMsgs int) {
	t.Helper()

	for i := 0; i < gameCount; i++ {
		game := fmt.Sprintf("Game%d", i+1)
		prefixChar := byte('A' + i)
		expect := make([]string, 0, perGameMsgs)
		for j := 1; j <= perGameMsgs; j++ {
			expect = append(expect, fmt.Sprintf("%c%d", prefixChar, j))
		}

		got := append([]string(nil), gotByKey[game]...)
		require.Equal(t, expect, got, "messages should be consumed in-order per game")
	}
}

func TestQueue_ProducerAndConsumer_Separated_ClusterClient(t *testing.T) {
	addrs := redisClusterAddrsFromEnv(t)
	producerClient, consumerClient := newClusterClients(t, addrs)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Unique prefix to avoid cross-test contamination in shared clusters.
	prefix := newIntegrationPrefix()

	// Producer side knows what it will publish; consumer side discovers queues via triggers.
	const (
		gameCount   = 14
		perGameMsgs = 5
	)

	consumer := startDynamicConsumer(ctx, t, consumerClient, prefix, func(q string) bool {
		return strings.HasPrefix(q, "Game")
	})
	t.Cleanup(consumer.closePubSub)

	// ---- Producer (Process A): publish in-order per queue ----
	runProducer(ctx, t, producerClient, prefix, gameCount, perGameMsgs)
	publishProducerDone(ctx, t, producerClient, prefix)

	consumer.waitDoneOrFail(ctx, t)
	consumer.waitStarterOrFail(ctx, t)
	consumer.wg.Wait()

	consumer.gotMu.Lock()
	gotCopy := make(map[string][]string, len(consumer.gotByKey))
	for k, v := range consumer.gotByKey {
		gotCopy[k] = append([]string(nil), v...)
	}
	consumer.gotMu.Unlock()

	assertOrderedPerGame(t, gotCopy, gameCount, perGameMsgs)
	require.Greater(t, atomic.LoadInt64(&consumer.maxInflight), int64(1))
}
