package redowl

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestWorkerPool(t *testing.T) {
	s, client := newTestRedis(t)
	_ = s

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("test_pool_%d", time.Now().UnixNano())

	var processed int64
	const (
		queueCount = 10
		msgsPerQ   = 3
	)

	// 创建 worker 池：少量 worker 处理多个队列
	pool := NewWorkerPool(
		client,
		prefix,
		func(ctx context.Context, queueName string, msg *Message) error {
			atomic.AddInt64(&processed, 1)
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

	// 模拟多个队列发送消息
	for i := 0; i < queueCount; i++ {
		queueName := fmt.Sprintf("queue_%d", i)
		q, err := New(client, queueName,
			WithPrefix(prefix),
			WithVisibilityTimeout(2*time.Second),
		)
		require.NoError(t, err)

		for j := 0; j < msgsPerQ; j++ {
			_, err := q.Send(ctx, []byte(fmt.Sprintf("msg_%d", j)), nil)
			require.NoError(t, err)
		}
	}

	// 等待处理完成
	want := int64(queueCount * msgsPerQ)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processed) >= want {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, want, atomic.LoadInt64(&processed))
}

func ExampleWorkerPool() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 10 个 worker 处理所有队列
	pool := NewWorkerPool(
		client,
		"myapp",
		func(ctx context.Context, queueName string, msg *Message) error {
			fmt.Printf("Queue %s: %s\n", queueName, string(msg.Body))
			return nil
		},
		WithWorkerCount(2),
		WithWorkerIdleTimeout(200*time.Millisecond),
		WithIdleTimeout(800*time.Millisecond),
		WithPollInterval(50*time.Millisecond),
	)
	_ = pool.Start(ctx)
	defer pool.Stop()

	// 动态创建队列并发送消息
	for i := 0; i < 5; i++ {
		q, _ := New(client, fmt.Sprintf("game_%d", i),
			WithPrefix("myapp"),
		)
		q.Send(ctx, []byte("player_joined"), nil)
	}

	// 2 个 worker 会自动处理所有队列的消息
	time.Sleep(200 * time.Millisecond)
}
