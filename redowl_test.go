package redowl_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	redowl "github.com/aura-studio/redowl"
)

const defaultPrefix = "redowl"

func readyKey(prefix, name string) string { return prefix + ":" + name + ":ready" }
func dlqKey(prefix, name string) string   { return prefix + ":" + name + ":dlq" }
func msgKey(prefix, name, id string) string {
	return prefix + ":" + name + ":msg:" + id
}
func receiptMapKey(prefix, name string) string { return prefix + ":" + name + ":receipt" }
func inflightKey(prefix, name string) string   { return prefix + ":" + name + ":inflight" }

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = c.Close(); s.Close() })
	return s, c
}

// ---- Simple / static tests ----

// Redis Cluster does not support PSUBSCRIBE.
// This regression test ensures redowl's non-test code does not start using PSubscribe.
func TestNoPSubscribeUsage(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			continue
		}

		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("ReadFile %s: %v", name, err)
		}

		// We intentionally check for the Go-redis method call form.
		if bytes.Contains(b, []byte(".PSubscribe(")) {
			t.Fatalf("%s uses PSubscribe; Redis Cluster does not support PSUBSCRIBE", filepath.Base(name))
		}
	}
}

// ---- Queue: basic -> advanced ----

func TestQueue_SendReceiveAck(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q1", redowl.WithVisibilityTimeout(200*time.Millisecond))
	require.NoError(t, err)

	id, err := q.Send(ctx, []byte("hello"), map[string]string{"k": "v"})
	require.NoError(t, err)
	require.NotEmpty(t, id)

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)
	require.Equal(t, id, m.ID)
	require.Equal(t, []byte("hello"), m.Body)
	require.Equal(t, "v", m.Attributes["k"])
	require.NotEmpty(t, m.ReceiptHandle)
	require.EqualValues(t, 1, m.ReceiveCount)

	require.NoError(t, q.Ack(ctx, m.ReceiptHandle))

	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m2)
}

func TestQueue_Ack_InvalidReceipt(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_ack_invalid")
	require.NoError(t, err)

	require.ErrorIs(t, q.Ack(ctx, ""), redowl.ErrInvalidReceiptHandle)
	require.ErrorIs(t, q.Ack(ctx, "   "), redowl.ErrInvalidReceiptHandle)
	require.ErrorIs(t, q.Ack(ctx, "nope"), redowl.ErrInvalidReceiptHandle)
}

func TestQueue_ChangeVisibility_InvalidReceipt(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_cv_invalid")
	require.NoError(t, err)

	require.ErrorIs(t, q.ChangeVisibility(ctx, "", 10*time.Millisecond), redowl.ErrInvalidReceiptHandle)
	require.ErrorIs(t, q.ChangeVisibility(ctx, "nope", 10*time.Millisecond), redowl.ErrInvalidReceiptHandle)
}

func TestQueue_VisibilityTimeout_Requeue(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q2", redowl.WithVisibilityTimeout(50*time.Millisecond))
	require.NoError(t, err)

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)

	time.Sleep(70 * time.Millisecond)
	requeued, err := q.RequeueExpiredOnce(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, requeued)

	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)
	require.Equal(t, m.ID, m2.ID)
	require.EqualValues(t, 2, m2.ReceiveCount)
}

func TestQueue_Receive_SkipsOrphanedReadyID(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_orphan_ready")
	require.NoError(t, err)

	// Push an ID without creating the message hash.
	require.NoError(t, c.RPush(ctx, readyKey(defaultPrefix, q.Name()), "missing_msg").Err())

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m)
}

func TestQueue_RequeueExpiredOnce_SkipsOrphanedReceipt(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_orphan_receipt", redowl.WithVisibilityTimeout(10*time.Millisecond))
	require.NoError(t, err)

	// Create inflight receipt mapping to a missing message id.
	require.NoError(t, c.HSet(ctx, receiptMapKey(defaultPrefix, q.Name()), "rh1", "missing_msg").Err())
	require.NoError(t, c.ZAdd(ctx, inflightKey(defaultPrefix, q.Name()), redis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: "rh1"}).Err())

	requeued, err := q.RequeueExpiredOnce(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, requeued)

	// Receipt should be cleaned up.
	_, err = c.HGet(ctx, receiptMapKey(defaultPrefix, q.Name()), "rh1").Result()
	require.Error(t, err)

	// And it should NOT push missing_msg into ready.
	ids, err := c.LRange(ctx, readyKey(defaultPrefix, q.Name()), 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, "missing_msg")
}

func TestQueue_DLQ(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q3", redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(2))
	require.NoError(t, err)

	id, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m1, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)
	require.Equal(t, id, m1.ID)

	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)

	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)
	require.EqualValues(t, 2, m2.ReceiveCount)

	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)

	m3, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m3, "should be moved to DLQ, not delivered")

	dlqIDs, err := c.LRange(ctx, dlqKey(defaultPrefix, q.Name()), 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, dlqIDs, id)
}

func TestQueue_DLQConsumeAndAck(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_dlq_consume", redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(1))
	require.NoError(t, err)

	id, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	// First receive puts it in-flight and increments rc to 1.
	m1, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)
	require.Equal(t, id, m1.ID)

	// Let it expire and requeue.
	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)

	// Second receive exceeds MaxReceiveCount(1) and moves it to DLQ.
	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m2)

	dlqMsg, err := q.ReceiveDLQ(ctx)
	require.NoError(t, err)
	require.NotNil(t, dlqMsg)
	require.Equal(t, id, dlqMsg.ID)
	require.NotEmpty(t, dlqMsg.ReceiptHandle)

	// Ack should delete the message.
	require.NoError(t, q.Ack(ctx, dlqMsg.ReceiptHandle))

	exists, err := c.Exists(ctx, msgKey(defaultPrefix, q.Name(), id)).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, exists)

	// DLQ should now be empty.
	dlqMsg2, err := q.ReceiveDLQ(ctx)
	require.NoError(t, err)
	require.Nil(t, dlqMsg2)
}

func TestQueue_DLQRedrive(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_dlq_redrive", redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(1))
	require.NoError(t, err)

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	_, _ = q.Receive(ctx)
	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)
	_, _ = q.Receive(ctx) // move to DLQ

	moved, err := q.RedriveDLQ(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, moved)

	// DLQ should be empty now.
	dlqMsg, err := q.ReceiveDLQ(ctx)
	require.NoError(t, err)
	require.Nil(t, dlqMsg)

	// Message should be available again in ready.
	msg, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.EqualValues(t, 1, msg.ReceiveCount)
}

func TestQueue_RedriveDLQ_SkipsOrphanedMessageID(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_redrive_orphan")
	require.NoError(t, err)

	// Put an ID into DLQ without creating msg hash.
	require.NoError(t, c.RPush(ctx, dlqKey(defaultPrefix, q.Name()), "missing_msg").Err())

	moved, err := q.RedriveDLQ(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, moved)

	// It should be removed from DLQ and NOT remain in ready.
	dlq, err := c.LLen(ctx, dlqKey(defaultPrefix, q.Name())).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, dlq)

	readyIDs, err := c.LRange(ctx, readyKey(defaultPrefix, q.Name()), 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, readyIDs, "missing_msg")
}

func TestQueue_ChangeVisibility_ExtendThenShorten(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_change_vis", redowl.WithVisibilityTimeout(200*time.Millisecond))
	require.NoError(t, err)

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)

	// Extend visibility: should not requeue after original timeout.
	require.NoError(t, q.ChangeVisibility(ctx, m.ReceiptHandle, 400*time.Millisecond))
	time.Sleep(220 * time.Millisecond)
	requeued, err := q.RequeueExpiredOnce(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, requeued)

	// Shorten visibility: should requeue soon.
	require.NoError(t, q.ChangeVisibility(ctx, m.ReceiptHandle, 20*time.Millisecond))
	time.Sleep(40 * time.Millisecond)
	requeued, err = q.RequeueExpiredOnce(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, requeued)

	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)
	require.Equal(t, m.ID, m2.ID)
	require.EqualValues(t, 2, m2.ReceiveCount)
}

func TestQueue_ReceiveWithWait_BlocksUntilMessage(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_wait")
	require.NoError(t, err)

	got := make(chan *redowl.Message, 1)
	go func() {
		m, _ := q.ReceiveWithWait(ctx, 500*time.Millisecond)
		got <- m
	}()

	// Ensure goroutine is likely waiting.
	time.Sleep(50 * time.Millisecond)
	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	select {
	case m := <-got:
		require.NotNil(t, m)
	case <-time.After(800 * time.Millisecond):
		t.Fatal("timeout waiting for ReceiveWithWait")
	}
}

func TestQueue_ReceiveDLQWithWait_BlocksUntilDLQ(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_dlq_wait", redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(1))
	require.NoError(t, err)

	got := make(chan *redowl.Message, 1)
	go func() {
		m, _ := q.ReceiveDLQWithWait(ctx, 800*time.Millisecond)
		got <- m
	}()

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)
	_, _ = q.Receive(ctx)
	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)
	_, _ = q.Receive(ctx) // move to DLQ

	select {
	case m := <-got:
		require.NotNil(t, m)
		require.NotEmpty(t, m.ReceiptHandle)
	case <-time.After(1200 * time.Millisecond):
		t.Fatal("timeout waiting for ReceiveDLQWithWait")
	}
}

func TestQueue_Reaper_RequeuesExpiredWithoutManualCall(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := redowl.New(c, "q_reaper", redowl.WithVisibilityTimeout(20*time.Millisecond), redowl.WithReaperInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer q.StopReaper()

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)

	// The reaper should eventually move it back to ready.
	require.Eventually(t, func() bool {
		ll, err := c.LLen(ctx, readyKey(defaultPrefix, q.Name())).Result()
		return err == nil && ll > 0
	}, 1500*time.Millisecond, 10*time.Millisecond)
}

func TestQueue_Triggers(t *testing.T) {
	_, c := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q, err := redowl.New(c, "q4", redowl.WithTriggerClient(c), redowl.WithVisibilityTimeout(100*time.Millisecond))
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		evs []redowl.EventType
	)
	unsub, err := q.Subscribe(ctx, func(e redowl.Event) {
		mu.Lock()
		defer mu.Unlock()
		evs = append(evs, e.Type)
	})
	require.NoError(t, err)
	defer func() { _ = unsub() }()

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)
	require.NoError(t, q.Ack(ctx, m.ReceiptHandle))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		seen := map[redowl.EventType]bool{}
		for _, t := range evs {
			seen[t] = true
		}
		return seen[redowl.EventSent] && seen[redowl.EventReceived] && seen[redowl.EventAcked]
	}, 1500*time.Millisecond, 20*time.Millisecond)
}

func TestQueue_Triggers_AutoDetectClient(t *testing.T) {
	_, c := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q, err := redowl.New(c, "q5", redowl.WithVisibilityTimeout(100*time.Millisecond))
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		evs []redowl.EventType
	)
	unsub, err := q.Subscribe(ctx, func(e redowl.Event) {
		mu.Lock()
		defer mu.Unlock()
		evs = append(evs, e.Type)
	})
	require.NoError(t, err)
	defer func() { _ = unsub() }()

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)
	require.NoError(t, q.Ack(ctx, m.ReceiptHandle))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		seen := map[redowl.EventType]bool{}
		for _, t := range evs {
			seen[t] = true
		}
		return seen[redowl.EventSent] && seen[redowl.EventReceived] && seen[redowl.EventAcked]
	}, 1500*time.Millisecond, 20*time.Millisecond)
}

func TestQueue_DLQTriggers(t *testing.T) {
	_, c := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q, err := redowl.New(c, "q_dlq_trigger", redowl.WithTriggerClient(c), redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(1))
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		evs []redowl.EventType
	)
	unsub, err := q.Subscribe(ctx, func(e redowl.Event) {
		mu.Lock()
		defer mu.Unlock()
		evs = append(evs, e.Type)
	})
	require.NoError(t, err)
	defer func() { _ = unsub() }()

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	_, _ = q.Receive(ctx)
	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)
	_, _ = q.Receive(ctx) // move to DLQ

	dlqMsg, err := q.ReceiveDLQ(ctx)
	require.NoError(t, err)
	require.NotNil(t, dlqMsg)
	require.NoError(t, q.Ack(ctx, dlqMsg.ReceiptHandle))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		seen := map[redowl.EventType]bool{}
		for _, t := range evs {
			seen[t] = true
		}
		return seen[redowl.EventToDLQ] && seen[redowl.EventDLQReceived] && seen[redowl.EventAcked]
	}, 1500*time.Millisecond, 20*time.Millisecond)
}

func TestQueue_DLQRedrive_Triggers(t *testing.T) {
	_, c := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q, err := redowl.New(c, "q_dlq_redrive_trigger", redowl.WithTriggerClient(c), redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(1))
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		evs []redowl.EventType
	)
	unsub, err := q.Subscribe(ctx, func(e redowl.Event) {
		mu.Lock()
		defer mu.Unlock()
		evs = append(evs, e.Type)
	})
	require.NoError(t, err)
	defer func() { _ = unsub() }()

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	_, _ = q.Receive(ctx)
	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)
	_, _ = q.Receive(ctx) // move to DLQ

	_, err = q.RedriveDLQ(ctx, 10)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		seen := map[redowl.EventType]bool{}
		for _, t := range evs {
			seen[t] = true
		}
		return seen[redowl.EventToDLQ] && seen[redowl.EventDLQRedriven]
	}, 1500*time.Millisecond, 20*time.Millisecond)
}

func TestQueue_ProducerConsumer_SeparateClients(t *testing.T) {
	s := miniredis.RunT(t)
	producerClient := redis.NewClient(&redis.Options{Addr: s.Addr()})
	consumerClient := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() {
		_ = producerClient.Close()
		_ = consumerClient.Close()
		s.Close()
	})

	ctx := context.Background()
	producer, err := redowl.New(producerClient, "q_pc")
	require.NoError(t, err)
	consumer, err := redowl.New(consumerClient, "q_pc")
	require.NoError(t, err)

	const n = 20
	for i := 0; i < n; i++ {
		_, err := producer.Send(ctx, []byte("m"), map[string]string{"i": strconv.Itoa(i)})
		require.NoError(t, err)
	}

	received := 0
	for received < n {
		m, err := consumer.ReceiveWithWait(ctx, 500*time.Millisecond)
		require.NoError(t, err)
		require.NotNil(t, m)
		require.NoError(t, consumer.Ack(ctx, m.ReceiptHandle))
		received++
	}

	// Queue should be empty.
	m, err := consumer.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m)
}

func TestQueue_SeparateClients_VisibilityRedelivery(t *testing.T) {
	s := miniredis.RunT(t)
	producerClient := redis.NewClient(&redis.Options{Addr: s.Addr()})
	consumer1Client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	consumer2Client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() {
		_ = producerClient.Close()
		_ = consumer1Client.Close()
		_ = consumer2Client.Close()
		s.Close()
	})

	ctx := context.Background()
	producer, err := redowl.New(producerClient, "q_pc_vis", redowl.WithVisibilityTimeout(30*time.Millisecond))
	require.NoError(t, err)
	consumer1, err := redowl.New(consumer1Client, "q_pc_vis", redowl.WithVisibilityTimeout(30*time.Millisecond))
	require.NoError(t, err)
	consumer2, err := redowl.New(consumer2Client, "q_pc_vis", redowl.WithVisibilityTimeout(30*time.Millisecond))
	require.NoError(t, err)

	id, err := producer.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	// Consumer1 receives but does NOT ack (simulating crash).
	m1, err := consumer1.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)
	require.Equal(t, id, m1.ID)

	// After visibility timeout, it should be re-deliverable.
	time.Sleep(50 * time.Millisecond)
	_, err = consumer2.RequeueExpiredOnce(ctx, 100)
	require.NoError(t, err)

	m2, err := consumer2.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)
	require.Equal(t, id, m2.ID)
	require.GreaterOrEqual(t, m2.ReceiveCount, int64(2))
	require.NoError(t, consumer2.Ack(ctx, m2.ReceiptHandle))
}

func TestQueue_ProducerA_ConsumerB_MultiGame_OrderedPerGame_ConcurrentAcrossGames(t *testing.T) {
	// Process A: producerClient
	// Process B: consumerClient
	s := miniredis.RunT(t)
	producerClient := redis.NewClient(&redis.Options{Addr: s.Addr()})
	consumerClient := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() {
		_ = producerClient.Close()
		_ = consumerClient.Close()
		s.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A->Game1: A1..A5, A->Game2: B1..B5, ... A->Game14: N1..N5
	const (
		gameCount   = 14
		perGameMsgs = 5
	)

	producerQueues := make([]*redowl.Queue, 0, gameCount)
	consumerQueues := make([]*redowl.Queue, 0, gameCount)
	for i := 0; i < gameCount; i++ {
		qName := fmt.Sprintf("Game%d", i+1)
		pq, err := redowl.New(producerClient, qName, redowl.WithVisibilityTimeout(200*time.Millisecond))
		require.NoError(t, err)
		cq, err := redowl.New(consumerClient, qName, redowl.WithVisibilityTimeout(200*time.Millisecond))
		require.NoError(t, err)
		producerQueues = append(producerQueues, pq)
		consumerQueues = append(consumerQueues, cq)
	}

	// Process A produces.
	for i := 0; i < gameCount; i++ {
		prefix := byte('A' + i)
		for j := 1; j <= perGameMsgs; j++ {
			payload := fmt.Sprintf("%c%d", prefix, j)
			_, err := producerQueues[i].Send(ctx, []byte(payload), map[string]string{"game": strconv.Itoa(i + 1)})
			require.NoError(t, err)
		}
	}

	// Process B consumes all games.
	var (
		mu          sync.Mutex
		gotByGame   = make(map[string][]string, gameCount)
		inflight    int64
		maxInflight int64
	)

	processDelay := 30 * time.Millisecond

	updateMax := func(v int64) {
		for {
			cur := atomic.LoadInt64(&maxInflight)
			if v <= cur {
				return
			}
			if atomic.CompareAndSwapInt64(&maxInflight, cur, v) {
				return
			}
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < gameCount; i++ {
		q := consumerQueues[i]
		wg.Add(1)
		go func(q *redowl.Queue) {
			defer wg.Done()
			for k := 0; k < perGameMsgs; k++ {
				msg, err := q.ReceiveWithWait(ctx, 2*time.Second)
				require.NoError(t, err)
				require.NotNil(t, msg)

				// Each message handling runs in its own goroutine,
				// but we wait for completion to guarantee per-queue ordering.
				done := make(chan struct{})
				go func(m *redowl.Message) {
					cur := atomic.AddInt64(&inflight, 1)
					updateMax(cur)
					defer atomic.AddInt64(&inflight, -1)

					// simulate work
					time.Sleep(processDelay)

					mu.Lock()
					gotByGame[q.Name()] = append(gotByGame[q.Name()], string(m.Body))
					mu.Unlock()

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

	wg.Wait()

	// Verify ordering per game.
	for i := 0; i < gameCount; i++ {
		game := fmt.Sprintf("Game%d", i+1)
		prefix := byte('A' + i)
		expect := make([]string, 0, perGameMsgs)
		for j := 1; j <= perGameMsgs; j++ {
			expect = append(expect, fmt.Sprintf("%c%d", prefix, j))
		}

		mu.Lock()
		got := append([]string(nil), gotByGame[game]...)
		mu.Unlock()

		require.Equal(t, expect, got, "messages should be consumed in-order per game")
	}

	// Verify different games can overlap (at least 2 handlers in flight at some point).
	require.Greater(t, atomic.LoadInt64(&maxInflight), int64(1))

	// Best-effort: queues should be drained.
	for i := 0; i < gameCount; i++ {
		q := consumerQueues[i]
		ll, err := consumerClient.LLen(ctx, readyKey(defaultPrefix, q.Name())).Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, ll)
		dlq, err := consumerClient.LLen(ctx, dlqKey(defaultPrefix, q.Name())).Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, dlq)
		z, err := consumerClient.ZCard(ctx, inflightKey(defaultPrefix, q.Name())).Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, z)
		h, err := consumerClient.HLen(ctx, receiptMapKey(defaultPrefix, q.Name())).Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, h)
	}
}

// ---- WorkerPool: basic -> advanced ----

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
	pool := redowl.NewWorkerPool(
		client,
		prefix,
		func(ctx context.Context, queueName string, msg *redowl.Message) error {
			atomic.AddInt64(&processed, 1)
			return nil
		},
		redowl.WithMinWorkers(0),
		redowl.WithMaxWorkers(5),
		redowl.WithWorkerIdleTimeout(200*time.Millisecond),
		redowl.WithQueueIdleTimeout(800*time.Millisecond),
		redowl.WithPollInterval(50*time.Millisecond),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	// 模拟多个队列发送消息
	for i := 0; i < queueCount; i++ {
		queueName := fmt.Sprintf("queue_%d", i)
		q, err := redowl.New(client, queueName,
			redowl.WithPrefix(prefix),
			redowl.WithVisibilityTimeout(2*time.Second),
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
	pool := redowl.NewWorkerPool(
		client,
		"myapp",
		func(ctx context.Context, queueName string, msg *redowl.Message) error {
			fmt.Printf("Queue %s: %s\n", queueName, string(msg.Body))
			return nil
		},
		redowl.WithMaxWorkers(2),
		redowl.WithWorkerIdleTimeout(200*time.Millisecond),
		redowl.WithQueueIdleTimeout(800*time.Millisecond),
		redowl.WithPollInterval(50*time.Millisecond),
	)
	_ = pool.Start(ctx)
	defer pool.Stop()

	// 动态创建队列并发送消息
	for i := 0; i < 5; i++ {
		q, _ := redowl.New(client, fmt.Sprintf("game_%d", i),
			redowl.WithPrefix("myapp"),
		)
		q.Send(ctx, []byte("player_joined"), nil)
	}

	// 2 个 worker 会自动处理所有队列的消息
	time.Sleep(200 * time.Millisecond)
}

func TestWorkerPool_DynamicRelease(t *testing.T) {
	_, client := newTestRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("test_dynamic_%d", time.Now().UnixNano())

	var processed int64

	pool := redowl.NewWorkerPool(
		client,
		prefix,
		func(ctx context.Context, queueName string, msg *redowl.Message) error {
			atomic.AddInt64(&processed, 1)
			time.Sleep(5 * time.Millisecond)
			return nil
		},
		redowl.WithMinWorkers(0),
		redowl.WithMaxWorkers(5),
		redowl.WithWorkerIdleTimeout(200*time.Millisecond),
		redowl.WithQueueIdleTimeout(800*time.Millisecond),
		redowl.WithPollInterval(50*time.Millisecond),
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
		q, err := redowl.New(client, fmt.Sprintf("wave1_q%d", i),
			redowl.WithPrefix(prefix),
			redowl.WithVisibilityTimeout(2*time.Second),
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
		live := pool.LiveWorkers()
		if live == 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	liveAfterIdle := pool.LiveWorkers()
	require.Equal(t, 0, liveAfterIdle)

	// 阶段 3: 发送新的队列
	t.Log("Phase 3: Sending wave2")
	for i := 0; i < queuesPerWave; i++ {
		q, err := redowl.New(client, fmt.Sprintf("wave2_q%d", i),
			redowl.WithPrefix(prefix),
			redowl.WithVisibilityTimeout(5*time.Second),
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

	pool := redowl.NewWorkerPool(
		client,
		prefix,
		func(ctx context.Context, queueName string, msg *redowl.Message) error {
			atomic.AddInt64(&processed, 1)
			time.Sleep(2 * time.Millisecond)
			return nil
		},
		redowl.WithMinWorkers(0),
		redowl.WithMaxWorkers(5),
		redowl.WithWorkerIdleTimeout(200*time.Millisecond),
		redowl.WithQueueIdleTimeout(800*time.Millisecond),
		redowl.WithPollInterval(50*time.Millisecond),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	// 模拟滚动队列：持续 5 波，总共 25 个队列，50 条消息
	t.Log("Simulating rolling queues...")
	for wave := 0; wave < 5; wave++ {
		for i := 0; i < 5; i++ {
			qName := fmt.Sprintf("rolling_w%d_q%d", wave, i)
			q, err := redowl.New(client, qName,
				redowl.WithPrefix(prefix),
				redowl.WithVisibilityTimeout(2*time.Second),
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

// ---- Redis cluster integration tests (optional; skipped unless REDIS_CLUSTER_ADDRS is set) ----

const producerDoneEventType redowl.EventType = "producer_done"

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

		q, err := redowl.New(consumerClient, queueName, redowl.WithPrefix(prefix), redowl.WithVisibilityTimeout(5*time.Second))
		require.NoError(t, err)

		c.wg.Add(1)
		go func(q *redowl.Queue) {
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
				go func(m *redowl.Message) {
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
				var ev redowl.Event
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
				if ev.Type == redowl.EventSent {
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

	producerQueues := make([]*redowl.Queue, 0, gameCount)
	for i := 0; i < gameCount; i++ {
		qName := fmt.Sprintf("Game%d", i+1)
		pq, err := redowl.New(producerClient, qName, redowl.WithPrefix(prefix), redowl.WithVisibilityTimeout(5*time.Second))
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
	b, _ := json.Marshal(redowl.Event{Type: producerDoneEventType, AtUnixMs: time.Now().UnixMilli(), Extra: map[string]string{"prefix": prefix}})
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

func TestWorkerPool_ManyQueues_ClusterClient(t *testing.T) {
	addrs := redisClusterAddrsFromEnv(t)
	producerClient, consumerClient := newClusterClients(t, addrs)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := newIntegrationPrefix()

	const (
		queueCount  = 100 // 100 个队列
		workerCount = 10  // 只用 10 个 worker
		msgsPerQ    = 5
	)

	var processed int64
	var processedByQueue = make(map[string]int64)
	var mu sync.Mutex

	// 创建 worker 池
	pool := redowl.NewWorkerPool(
		consumerClient,
		prefix,
		func(ctx context.Context, queueName string, msg *redowl.Message) error {
			atomic.AddInt64(&processed, 1)
			mu.Lock()
			processedByQueue[queueName]++
			mu.Unlock()
			time.Sleep(10 * time.Millisecond) // 模拟处理
			return nil
		},
		redowl.WithMaxWorkers(workerCount),
		redowl.WithQueueIdleTimeout(2*time.Minute),
		redowl.WithPollInterval(200*time.Millisecond),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	// 向 100 个队列发送消息
	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("queue_%d", i)
		q, err := redowl.New(producerClient, qName,
			redowl.WithPrefix(prefix),
			redowl.WithVisibilityTimeout(5*time.Second),
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
