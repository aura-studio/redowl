//go:build rediscluster

package redowl

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool_ManyQueues_ClusterClient(t *testing.T) {
	addrs := redisClusterAddrsFromEnv(t)
	producerClient, consumerClient := newClusterClients(t, addrs)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := newIntegrationPrefix()

	const (
		queueCount  = 100  // 100 个队列
		workerCount = 10   // 只用 10 个 worker
		msgsPerQ    = 5
	)

	var processed int64
	var processedByQueue = make(map[string]int64)
	var mu sync.Mutex

	// 创建 worker 池
	pool := NewWorkerPool(
		consumerClient,
		prefix,
		func(ctx context.Context, queueName string, msg *Message) error {
			atomic.AddInt64(&processed, 1)
			mu.Lock()
			processedByQueue[queueName]++
			mu.Unlock()
			time.Sleep(10 * time.Millisecond) // 模拟处理
			return nil
		},
		WithWorkerCount(workerCount),
		WithIdleTimeout(2*time.Minute),
		WithPollInterval(200*time.Millisecond),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	// 向 100 个队列发送消息
	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("queue_%d", i)
		q, err := New(producerClient, qName,
			WithPrefix(prefix),
			WithVisibilityTimeout(5*time.Second),
		)
		require.NoError(t, err)

		for j := 0; j < msgsPerQ; j++ {
			_, err := q.Send(ctx, []byte(fmt.Sprintf("msg_%d", j)), nil)
			require.NoError(t, err)
		}
	}

	// 等待所有消息处理完成
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processed) >= int64(queueCount*msgsPerQ) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	finalProcessed := atomic.LoadInt64(&processed)
	require.Equal(t, int64(queueCount*msgsPerQ), finalProcessed,
		"should process all messages with worker pool")

	mu.Lock()
	require.Len(t, processedByQueue, queueCount,
		"should process messages from all queues")
	mu.Unlock()

	stats := pool.Stats()
	t.Logf("Worker pool stats: %d queues tracked", len(stats))
	t.Logf("Total messages processed: %d", finalProcessed)
	t.Logf("Workers used: %d (vs %d queues)", workerCount, queueCount)
}
