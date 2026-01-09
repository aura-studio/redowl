package redowl

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = c.Close(); s.Close() })
	return s, c
}

func TestQueue_SendReceiveAck(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q1", WithVisibilityTimeout(200*time.Millisecond))
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

func TestQueue_VisibilityTimeout_Requeue(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q2", WithVisibilityTimeout(50*time.Millisecond))
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

func TestQueue_DLQ(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q3", WithVisibilityTimeout(10*time.Millisecond), WithMaxReceiveCount(2))
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

	dlqIDs, err := c.LRange(ctx, q.dlqKey(), 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, dlqIDs, id)
}

func TestQueue_Triggers(t *testing.T) {
	_, c := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q, err := New(c, "q4", WithTriggerClient(c), WithVisibilityTimeout(100*time.Millisecond))
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		evs []EventType
	)
	unsub, err := q.Subscribe(ctx, func(e Event) {
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
		seen := map[EventType]bool{}
		for _, t := range evs {
			seen[t] = true
		}
		return seen[EventSent] && seen[EventReceived] && seen[EventAcked]
	}, 1500*time.Millisecond, 20*time.Millisecond)
}

func TestQueue_Triggers_AutoDetectClient(t *testing.T) {
	_, c := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q, err := New(c, "q5", WithVisibilityTimeout(100*time.Millisecond))
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		evs []EventType
	)
	unsub, err := q.Subscribe(ctx, func(e Event) {
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
		seen := map[EventType]bool{}
		for _, t := range evs {
			seen[t] = true
		}
		return seen[EventSent] && seen[EventReceived] && seen[EventAcked]
	}, 1500*time.Millisecond, 20*time.Millisecond)
}

func TestQueue_Receive_SkipsOrphanedReadyID(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q_orphan_ready")
	require.NoError(t, err)

	// Push an ID without creating the message hash.
	require.NoError(t, c.RPush(ctx, q.readyKey(), "missing_msg").Err())

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m)
}

func TestQueue_RequeueExpiredOnce_SkipsOrphanedReceipt(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q_orphan_receipt", WithVisibilityTimeout(10*time.Millisecond))
	require.NoError(t, err)

	// Create inflight receipt mapping to a missing message id.
	require.NoError(t, c.HSet(ctx, q.receiptMapKey(), "rh1", "missing_msg").Err())
	require.NoError(t, c.ZAdd(ctx, q.inflightKey(), redis.Z{Score: float64(time.Now().Add(-time.Second).UnixMilli()), Member: "rh1"}).Err())

	requeued, err := q.RequeueExpiredOnce(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 0, requeued)

	// Receipt should be cleaned up.
	_, err = c.HGet(ctx, q.receiptMapKey(), "rh1").Result()
	require.Error(t, err)

	// And it should NOT push missing_msg into ready.
	ids, err := c.LRange(ctx, q.readyKey(), 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, "missing_msg")
}

func TestQueue_DLQConsumeAndAck(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q_dlq_consume", WithVisibilityTimeout(10*time.Millisecond), WithMaxReceiveCount(1))
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

	exists, err := c.Exists(ctx, q.msgKey(id)).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, exists)

	// DLQ should now be empty.
	dlqMsg2, err := q.ReceiveDLQ(ctx)
	require.NoError(t, err)
	require.Nil(t, dlqMsg2)
}

func TestQueue_DLQTriggers(t *testing.T) {
	_, c := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q, err := New(c, "q_dlq_trigger", WithTriggerClient(c), WithVisibilityTimeout(10*time.Millisecond), WithMaxReceiveCount(1))
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		evs []EventType
	)
	unsub, err := q.Subscribe(ctx, func(e Event) {
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
		seen := map[EventType]bool{}
		for _, t := range evs {
			seen[t] = true
		}
		return seen[EventToDLQ] && seen[EventDLQReceived] && seen[EventAcked]
	}, 1500*time.Millisecond, 20*time.Millisecond)
}

func TestQueue_DLQRedrive(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q_dlq_redrive", WithVisibilityTimeout(10*time.Millisecond), WithMaxReceiveCount(1))
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

func TestQueue_DLQRedrive_Triggers(t *testing.T) {
	_, c := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q, err := New(c, "q_dlq_redrive_trigger", WithTriggerClient(c), WithVisibilityTimeout(10*time.Millisecond), WithMaxReceiveCount(1))
	require.NoError(t, err)

	var (
		mu  sync.Mutex
		evs []EventType
	)
	unsub, err := q.Subscribe(ctx, func(e Event) {
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
		seen := map[EventType]bool{}
		for _, t := range evs {
			seen[t] = true
		}
		return seen[EventToDLQ] && seen[EventDLQRedriven]
	}, 1500*time.Millisecond, 20*time.Millisecond)
}

func TestQueue_Ack_InvalidReceipt(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q_ack_invalid")
	require.NoError(t, err)

	require.ErrorIs(t, q.Ack(ctx, ""), ErrInvalidReceiptHandle)
	require.ErrorIs(t, q.Ack(ctx, "   "), ErrInvalidReceiptHandle)
	require.ErrorIs(t, q.Ack(ctx, "nope"), ErrInvalidReceiptHandle)
}

func TestQueue_ChangeVisibility_InvalidReceipt(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q_cv_invalid")
	require.NoError(t, err)

	require.ErrorIs(t, q.ChangeVisibility(ctx, "", 10*time.Millisecond), ErrInvalidReceiptHandle)
	require.ErrorIs(t, q.ChangeVisibility(ctx, "nope", 10*time.Millisecond), ErrInvalidReceiptHandle)
}

func TestQueue_ChangeVisibility_ExtendThenShorten(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q_change_vis", WithVisibilityTimeout(200*time.Millisecond))
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

	q, err := New(c, "q_wait")
	require.NoError(t, err)

	got := make(chan *Message, 1)
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

	q, err := New(c, "q_dlq_wait", WithVisibilityTimeout(10*time.Millisecond), WithMaxReceiveCount(1))
	require.NoError(t, err)

	got := make(chan *Message, 1)
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

	q, err := New(c, "q_reaper", WithVisibilityTimeout(20*time.Millisecond), WithReaperInterval(10*time.Millisecond))
	require.NoError(t, err)
	defer q.StopReaper()

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)

	// The reaper should eventually move it back to ready.
	require.Eventually(t, func() bool {
		ll, err := c.LLen(ctx, q.readyKey()).Result()
		return err == nil && ll > 0
	}, 1500*time.Millisecond, 10*time.Millisecond)
}

func TestQueue_RedriveDLQ_SkipsOrphanedMessageID(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q, err := New(c, "q_redrive_orphan")
	require.NoError(t, err)

	// Put an ID into DLQ without creating msg hash.
	require.NoError(t, c.RPush(ctx, q.dlqKey(), "missing_msg").Err())

	moved, err := q.RedriveDLQ(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 0, moved)

	// It should be removed from DLQ and NOT remain in ready.
	dlq, err := c.LLen(ctx, q.dlqKey()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, dlq)

	readyIDs, err := c.LRange(ctx, q.readyKey(), 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, readyIDs, "missing_msg")
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
	producer, err := New(producerClient, "q_pc")
	require.NoError(t, err)
	consumer, err := New(consumerClient, "q_pc")
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
	producer, err := New(producerClient, "q_pc_vis", WithVisibilityTimeout(30*time.Millisecond))
	require.NoError(t, err)
	consumer1, err := New(consumer1Client, "q_pc_vis", WithVisibilityTimeout(30*time.Millisecond))
	require.NoError(t, err)
	consumer2, err := New(consumer2Client, "q_pc_vis", WithVisibilityTimeout(30*time.Millisecond))
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

	producerQueues := make([]*Queue, 0, gameCount)
	consumerQueues := make([]*Queue, 0, gameCount)
	for i := 0; i < gameCount; i++ {
		qName := fmt.Sprintf("Game%d", i+1)
		pq, err := New(producerClient, qName, WithVisibilityTimeout(200*time.Millisecond))
		require.NoError(t, err)
		cq, err := New(consumerClient, qName, WithVisibilityTimeout(200*time.Millisecond))
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
		go func(q *Queue) {
			defer wg.Done()
			for k := 0; k < perGameMsgs; k++ {
				msg, err := q.ReceiveWithWait(ctx, 2*time.Second)
				require.NoError(t, err)
				require.NotNil(t, msg)

				// Each message handling runs in its own goroutine,
				// but we wait for completion to guarantee per-queue ordering.
				done := make(chan struct{})
				go func(m *Message) {
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
		ll, err := consumerClient.LLen(ctx, q.readyKey()).Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, ll)
		dlq, err := consumerClient.LLen(ctx, q.dlqKey()).Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, dlq)
		z, err := consumerClient.ZCard(ctx, q.inflightKey()).Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, z)
		h, err := consumerClient.HLen(ctx, q.receiptMapKey()).Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, h)
	}
}
