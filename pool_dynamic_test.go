package redowl

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool_DynamicRelease(t *testing.T) {
	_, client := newTestRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("test_dynamic_%d", time.Now().UnixNano())

	var processed int64

	pool := NewWorkerPool(
		client,
		prefix,
		func(ctx context.Context, queueName string, msg *Message) error {
			atomic.AddInt64(&processed, 1)
			time.Sleep(5 * time.Millisecond)
			return nil
		},
		WithMinWorkers(0),
		WithWorkerCount(5),
		WithWorkerIdleTimeout(200*time.Millisecond),
		WithIdleTimeout(800*time.Millisecond),
		WithPollInterval(50*time.Millisecond),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	const (
		queuesPerWave = 10
		msgsPerQueue  = 4
	)
	// 阶段 1: 发送一批队列
	t.Log("Phase 1: Sending wave1")
	for i := 0; i < queuesPerWave; i++ {
		q, err := New(client, fmt.Sprintf("wave1_q%d", i),
			WithPrefix(prefix),
			WithVisibilityTimeout(2*time.Second),
		)
		require.NoError(t, err)

		for j := 0; j < msgsPerQueue; j++ {
			_, err := q.Send(ctx, []byte(fmt.Sprintf("msg_%d", j)), nil)
			require.NoError(t, err)
		}
	}

	// 等待第一波处理完成
	wantPhase1 := int64(queuesPerWave * msgsPerQueue)
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processed) >= wantPhase1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	phase1Processed := atomic.LoadInt64(&processed)
	t.Logf("Phase 1 processed: %d messages", phase1Processed)
	require.Equal(t, wantPhase1, phase1Processed)

	// 阶段 2: 等待 worker 释放到 minWorkers
	t.Log("Phase 2: Waiting for workers to release...")
	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		pool.workersMu.Lock()
		live := pool.workersLive
		pool.workersMu.Unlock()
		if live == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	pool.workersMu.Lock()
	liveAfterIdle := pool.workersLive
	pool.workersMu.Unlock()
	require.Equal(t, 0, liveAfterIdle)

	// 阶段 3: 发送新的队列
	t.Log("Phase 3: Sending wave2")
	for i := 0; i < queuesPerWave; i++ {
		q, err := New(client, fmt.Sprintf("wave2_q%d", i),
			WithPrefix(prefix),
			WithVisibilityTimeout(5*time.Second),
		)
		require.NoError(t, err)

		for j := 0; j < msgsPerQueue; j++ {
			_, err := q.Send(ctx, []byte(fmt.Sprintf("msg_%d", j)), nil)
			require.NoError(t, err)
		}
	}

	// 等待第二波处理完成
	wantTotal := wantPhase1 * 2
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processed) >= wantTotal {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	phase2Processed := atomic.LoadInt64(&processed)
	t.Logf("Phase 2 processed: %d messages", phase2Processed)
	require.Equal(t, wantTotal, phase2Processed)
}

func TestWorkerPool_RollingQueues(t *testing.T) {
	_, client := newTestRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("test_rolling_%d", time.Now().UnixNano())

	var processed int64

	pool := NewWorkerPool(
		client,
		prefix,
		func(ctx context.Context, queueName string, msg *Message) error {
			atomic.AddInt64(&processed, 1)
			time.Sleep(2 * time.Millisecond)
			return nil
		},
		WithMinWorkers(0),
		WithWorkerCount(5),
		WithWorkerIdleTimeout(200*time.Millisecond),
		WithIdleTimeout(800*time.Millisecond),
		WithPollInterval(50*time.Millisecond),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	// 模拟滚动队列：持续 5 波，总共 25 个队列，50 条消息
	t.Log("Simulating rolling queues...")
	for wave := 0; wave < 5; wave++ {
		for i := 0; i < 5; i++ {
			qName := fmt.Sprintf("rolling_w%d_q%d", wave, i)
			q, err := New(client, qName,
				WithPrefix(prefix),
				WithVisibilityTimeout(2*time.Second),
			)
			require.NoError(t, err)

			for j := 0; j < 2; j++ {
				_, err := q.Send(ctx, []byte(fmt.Sprintf("msg_%d", j)), nil)
				require.NoError(t, err)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 等待所有消息处理完成
	want := int64(50)
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processed) >= want {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	finalProcessed := atomic.LoadInt64(&processed)
	t.Logf("Final processed: %d messages", finalProcessed)
	require.Equal(t, want, finalProcessed)

	stats := pool.Stats()
	t.Logf("Tracked queues: %d", len(stats))

	// 验证：worker 能够处理滚动队列
	require.GreaterOrEqual(t, len(stats), 20, "Should track most queues")
}
