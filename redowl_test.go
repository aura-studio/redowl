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
func namespaceEventChannel(prefix string) string {
	return prefix + ":events"
}

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()

	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() {
		_ = c.Close()
		s.Close()
	})
	return s, c
}

func mustNewQueue(t *testing.T, cmd redis.Cmdable, name string, opts ...redowl.Option) *redowl.Queue {
	t.Helper()

	q, err := redowl.New(cmd, name, opts...)
	require.NoError(t, err)
	return q
}

func requireEventually(t *testing.T, timeout time.Duration, interval time.Duration, fn func() bool, msgAndArgs ...any) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(interval)
	}
	require.Fail(t, "condition not met", msgAndArgs...)
}

// ---------------------------------
// Static / regression tests
// ---------------------------------

// TestStatic_NoPSubscribeUsage ensures production code never uses go-redis PSUBSCRIBE.
//
// Why:
// - Redis Cluster does not support PSUBSCRIBE.
// - The implementation intentionally relies on normal SUBSCRIBE with a namespace channel.
//
// This is a static scan over non-test .go files to catch accidental regressions.
func TestStatic_NoPSubscribeUsage(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

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
		require.NoError(t, err)

		if bytes.Contains(b, []byte(".PSubscribe(")) {
			t.Fatalf("%s uses PSubscribe; Redis Cluster does not support PSUBSCRIBE", filepath.Base(name))
		}
	}
}

// ---------------------------------
// Queue: basic -> advanced
// ---------------------------------

// TestQueue_New_InvalidName verifies queue name validation.
//
// The constructor trims spaces; empty or whitespace-only names must fail with ErrInvalidQueueName.
func TestQueue_New_InvalidName(t *testing.T) {
	_, c := newTestRedis(t)

	_, err := redowl.New(c, "")
	require.ErrorIs(t, err, redowl.ErrInvalidQueueName)

	_, err = redowl.New(c, "   ")
	require.ErrorIs(t, err, redowl.ErrInvalidQueueName)
}

// TestQueue_New_EmptyPrefixFallsBackToDefault verifies that an empty prefix is normalized.
//
// Passing WithPrefix("") should fall back to the default prefix ("redowl") so key layout stays valid.
func TestQueue_New_EmptyPrefixFallsBackToDefault(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_default_prefix", redowl.WithPrefix(""))

	id, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	exists, err := c.Exists(ctx, msgKey(defaultPrefix, q.Name(), id)).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, exists)
}

// TestQueue_SendReceiveAck_Basic verifies the happy path: Send -> Receive -> Ack.
//
// It asserts:
// - Attributes round-trip.
// - ReceiptHandle is generated.
// - ReceiveCount increments.
// - Ack deletes message data and prevents re-delivery.
func TestQueue_SendReceiveAck_Basic(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_basic", redowl.WithVisibilityTimeout(200*time.Millisecond))

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
	require.False(t, m.CreatedAt.IsZero())

	require.NoError(t, q.Ack(ctx, m.ReceiptHandle))

	// After Ack, the message should not be delivered again.
	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m2)

	// And message data should be cleaned up.
	exists, err := c.Exists(ctx, msgKey(defaultPrefix, q.Name(), id)).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, exists)
}

// TestQueue_Ack_InvalidReceiptHandle verifies Ack input validation and missing receipt behavior.
//
// Empty/whitespace receipts should be rejected immediately.
// Unknown receipts (no mapping in Redis) should return ErrInvalidReceiptHandle.
func TestQueue_Ack_InvalidReceiptHandle(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_ack_invalid")

	for _, rh := range []string{"", "   ", "nope"} {
		err := q.Ack(ctx, rh)
		require.ErrorIs(t, err, redowl.ErrInvalidReceiptHandle)
	}
}

// TestQueue_ChangeVisibility_InvalidReceiptHandle verifies ChangeVisibility input validation.
//
// ChangeVisibility is only meaningful for an existing in-flight receipt.
// Empty/unknown receipts must return ErrInvalidReceiptHandle.
func TestQueue_ChangeVisibility_InvalidReceiptHandle(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_cv_invalid")

	for _, rh := range []string{"", "nope"} {
		err := q.ChangeVisibility(ctx, rh, 10*time.Millisecond)
		require.ErrorIs(t, err, redowl.ErrInvalidReceiptHandle)
	}
}

// TestQueue_Receive_SkipsOrphanedReadyID verifies robustness against orphaned IDs in the ready list.
//
// A rare race/manual intervention can leave an ID in ready with the msg hash missing.
// Receive should skip such IDs and return nil (no message), rather than erroring or poisoning the queue.
func TestQueue_Receive_SkipsOrphanedReadyID(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_orphan_ready")

	// Push an ID without creating the message hash.
	require.NoError(t, c.RPush(ctx, readyKey(defaultPrefix, q.Name()), "missing_msg").Err())

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m)
}

// TestQueue_RequeueExpiredOnce_CleansOrphanedReceipt verifies robustness of the requeue logic.
//
// If inflight contains an expired receipt but the receipt->id mapping points to a missing message,
// RequeueExpiredOnce should:
// - clean up the inflight entry and receipt mapping
// - NOT push the missing message ID back to ready
func TestQueue_RequeueExpiredOnce_CleansOrphanedReceipt(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_orphan_receipt", redowl.WithVisibilityTimeout(10*time.Millisecond))

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

// TestQueue_VisibilityTimeout_RequeueExpiredOnce verifies visibility timeout semantics.
//
// If a message is received but not acked before VisibilityTimeout, calling RequeueExpiredOnce
// should move it back to ready so it can be received again, and ReceiveCount should increment.
func TestQueue_VisibilityTimeout_RequeueExpiredOnce(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_visibility", redowl.WithVisibilityTimeout(40*time.Millisecond))

	_, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m1, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)
	require.EqualValues(t, 1, m1.ReceiveCount)

	time.Sleep(60 * time.Millisecond)
	requeued, err := q.RequeueExpiredOnce(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, requeued)

	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)
	require.Equal(t, m1.ID, m2.ID)
	require.EqualValues(t, 2, m2.ReceiveCount)
}

// TestQueue_ChangeVisibility_CanExpireEarly verifies that visibility can be shortened.
//
// We receive a message with a long VisibilityTimeout, then ChangeVisibility to a past deadline.
// RequeueExpiredOnce should immediately make it receivable again.
func TestQueue_ChangeVisibility_CanExpireEarly(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_change_visibility", redowl.WithVisibilityTimeout(5*time.Second))

	_, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m1, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)

	// Make it immediately expired.
	require.NoError(t, q.ChangeVisibility(ctx, m1.ReceiptHandle, -time.Second))
	requeued, err := q.RequeueExpiredOnce(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, requeued)

	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)
	require.Equal(t, m1.ID, m2.ID)
	require.EqualValues(t, 2, m2.ReceiveCount)
}

// TestQueue_DLQ_MovesAfterMaxReceiveCount verifies DLQ routing.
//
// When MaxReceiveCount is set, a message exceeding that count should be moved to DLQ and not
// delivered from the normal Receive path.
func TestQueue_DLQ_MovesAfterMaxReceiveCount(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_dlq", redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(2))

	id, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m1, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)
	require.Equal(t, id, m1.ID)
	require.EqualValues(t, 1, m1.ReceiveCount)

	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)

	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m2)
	require.EqualValues(t, 2, m2.ReceiveCount)

	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)

	// Third receive exceeds MaxReceiveCount(2) and moves to DLQ, returning nil.
	m3, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m3)

	dlqIDs, err := c.LRange(ctx, dlqKey(defaultPrefix, q.Name()), 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, dlqIDs, id)
}

// TestQueue_DLQ_ReceiveAndAck_DeletesMessage verifies DLQ consumption and cleanup.
//
// DLQ receive produces a ReceiptHandle so the caller can Ack to delete the message data.
func TestQueue_DLQ_ReceiveAndAck_DeletesMessage(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_dlq_consume", redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(1))

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
	require.Nil(t, m2)

	dlqMsg, err := q.ReceiveDLQ(ctx)
	require.NoError(t, err)
	require.NotNil(t, dlqMsg)
	require.Equal(t, id, dlqMsg.ID)
	require.NotEmpty(t, dlqMsg.ReceiptHandle)

	require.NoError(t, q.Ack(ctx, dlqMsg.ReceiptHandle))

	exists, err := c.Exists(ctx, msgKey(defaultPrefix, q.Name(), id)).Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, exists)
}

// TestQueue_RedriveDLQ_MovesBackAndResetsReceiveCount verifies DLQ redrive behavior.
//
// RedriveDLQ moves message IDs back to ready and resets rc to 0 so the message can be processed again.
func TestQueue_RedriveDLQ_MovesBackAndResetsReceiveCount(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_dlq_redrive", redowl.WithVisibilityTimeout(10*time.Millisecond), redowl.WithMaxReceiveCount(1))

	id, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m1, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)

	time.Sleep(20 * time.Millisecond)
	_, _ = q.RequeueExpiredOnce(ctx, 100)

	m2, err := q.Receive(ctx)
	require.NoError(t, err)
	require.Nil(t, m2)

	moved, err := q.RedriveDLQ(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, moved)

	// It should be receivable again, and receive count should be reset.
	m3, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m3)
	require.Equal(t, id, m3.ID)
	require.EqualValues(t, 1, m3.ReceiveCount)
}

// TestQueue_ReceiveWithWait_SubSecondTimeoutReturnsNil verifies the sub-second wait behavior.
//
// For waits < 1 second, ReceiveWithWait polls with small sleeps up to the deadline.
// When no message arrives, it should return (nil, nil) after approximately the given duration.
func TestQueue_ReceiveWithWait_SubSecondTimeoutReturnsNil(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_wait_subsec")

	start := time.Now()
	m, err := q.ReceiveWithWait(ctx, 80*time.Millisecond)
	require.NoError(t, err)
	require.Nil(t, m)
	require.GreaterOrEqual(t, time.Since(start), 60*time.Millisecond)
}

// TestQueue_ReceiveWithWait_BLPopReceivesWhenMessageArrives verifies the >=1s wait behavior.
//
// For waits >= 1 second, ReceiveWithWait uses Redis BLPOP. When a message arrives during the wait,
// it should return that message.
func TestQueue_ReceiveWithWait_BLPopReceivesWhenMessageArrives(t *testing.T) {
	_, c := newTestRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	q := mustNewQueue(t, c, "q_wait_blpop")

	go func() {
		time.Sleep(120 * time.Millisecond)
		_, _ = q.Send(ctx, []byte("x"), nil)
	}()

	m, err := q.ReceiveWithWait(ctx, time.Second)
	require.NoError(t, err)
	require.NotNil(t, m)
	require.Equal(t, []byte("x"), m.Body)
}

// TestQueue_Reaper_RequeuesExpiredInFlight verifies the background reaper.
//
// WithReaperInterval starts a goroutine that periodically calls RequeueExpiredOnce.
// Here we receive a message (making it in-flight), don't ack it, and assert it becomes receivable again.
func TestQueue_Reaper_RequeuesExpiredInFlight(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	q := mustNewQueue(t, c, "q_reaper", redowl.WithVisibilityTimeout(15*time.Millisecond), redowl.WithReaperInterval(10*time.Millisecond))
	defer q.StopReaper()

	_, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	m1, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m1)

	requireEventually(t, 300*time.Millisecond, 10*time.Millisecond, func() bool {
		m2, err := q.Receive(ctx)
		if err != nil {
			return false
		}
		return m2 != nil && m2.ID == m1.ID && m2.ReceiveCount >= 2
	}, "expected message to be requeued and received again")
}

// TestSubscribe_ReturnsErrWhenTriggersNotConfigured verifies Subscribe requires triggers.
//
// We pass a redis.Cmdable that is NOT a redis.UniversalClient (pipeline), which prevents auto trigger
// configuration, and expect ErrTriggersNotConfigured.
func TestSubscribe_ReturnsErrWhenTriggersNotConfigured(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	// Use a cmd that is not redis.UniversalClient, so auto TriggerClient is not set.
	pipe := c.Pipeline()
	q := mustNewQueue(t, pipe, "q_no_triggers")

	_, err := q.Subscribe(ctx, func(redowl.Event) {})
	require.ErrorIs(t, err, redowl.ErrTriggersNotConfigured)
}

// TestEvents_PerQueueSubscribe_SeesLifecycleEvents verifies per-queue event subscription.
//
// When triggers are enabled, Send/Receive/Ack should publish events; Subscribe should receive them.
// This test asserts at least {sent, received, acked} are observed.
func TestEvents_PerQueueSubscribe_SeesLifecycleEvents(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	prefix := "ev_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	q := mustNewQueue(t, c, "q_events", redowl.WithPrefix(prefix), redowl.WithVisibilityTimeout(50*time.Millisecond))

	var gotMu sync.Mutex
	got := make([]redowl.EventType, 0, 8)
	unsub, err := q.Subscribe(ctx, func(e redowl.Event) {
		gotMu.Lock()
		got = append(got, e.Type)
		gotMu.Unlock()
	})
	require.NoError(t, err)
	defer func() { _ = unsub() }()

	id, err := q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)
	require.NotEmpty(t, id)

	m, err := q.Receive(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)
	require.NoError(t, q.Ack(ctx, m.ReceiptHandle))

	requireEventually(t, 500*time.Millisecond, 10*time.Millisecond, func() bool {
		gotMu.Lock()
		defer gotMu.Unlock()
		hasSent := false
		hasReceived := false
		hasAcked := false
		for _, et := range got {
			if et == redowl.EventSent {
				hasSent = true
			}
			if et == redowl.EventReceived {
				hasReceived = true
			}
			if et == redowl.EventAcked {
				hasAcked = true
			}
		}
		return hasSent && hasReceived && hasAcked
	}, "expected sent/received/acked events")
}

// TestEvents_NamespaceChannel_IncludesQueueName verifies the namespace event channel.
//
// redowl publishes each event to both:
// - per-queue channel: {prefix}:{queue}:events
// - namespace channel: {prefix}:events
//
// The namespace channel must include the Queue field so consumers can dynamically discover queues.
func TestEvents_NamespaceChannel_IncludesQueueName(t *testing.T) {
	_, c := newTestRedis(t)
	ctx := context.Background()

	prefix := "ns_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	q := mustNewQueue(t, c, "q_namespace", redowl.WithPrefix(prefix))

	pubsub := c.Subscribe(ctx, namespaceEventChannel(prefix))
	t.Cleanup(func() { _ = pubsub.Close() })
	_, err := pubsub.Receive(ctx)
	require.NoError(t, err)

	_, err = q.Send(ctx, []byte("x"), nil)
	require.NoError(t, err)

	select {
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for namespace event")
	case msg := <-pubsub.Channel():
		var ev redowl.Event
		require.NoError(t, json.Unmarshal([]byte(msg.Payload), &ev))
		require.Equal(t, redowl.EventSent, ev.Type)
		require.Equal(t, q.Name(), ev.Queue)
		require.NotEmpty(t, ev.MessageID)
	}
}

// ---------------------------------
// WorkerPool
// ---------------------------------

// TestWorkerPool_ProcessesManyQueues_RespectsMaxWorkers_AndPerQueueOrder verifies WorkerPool core behavior.
//
// It asserts:
// - Many queues can be processed with a small worker cap.
// - LiveWorkers never exceeds MaxWorkers.
// - Within each queue, message order is preserved (the pool processes one message at a time per queue).
func TestWorkerPool_ProcessesManyQueues_RespectsMaxWorkers_AndPerQueueOrder(t *testing.T) {
	_, c := newTestRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := "wp_" + strconv.FormatInt(time.Now().UnixNano(), 10)

	const (
		queueCount  = 20
		msgsPerQ    = 3
		maxWorkers  = 4
		poll        = 20 * time.Millisecond
		queueIdle   = 500 * time.Millisecond
		workerIdle  = 200 * time.Millisecond
	)

	var processed int64
	byQueue := make(map[string][]string)
	var mu sync.Mutex

	pool := redowl.NewWorkerPool(
		c,
		prefix,
		func(ctx context.Context, queueName string, msg *redowl.Message) error {
			atomic.AddInt64(&processed, 1)
			mu.Lock()
			byQueue[queueName] = append(byQueue[queueName], string(msg.Body))
			mu.Unlock()
			return nil
		},
		redowl.WithMaxWorkers(maxWorkers),
		redowl.WithMinWorkers(0),
		redowl.WithQueueIdleTimeout(queueIdle),
		redowl.WithWorkerIdleTimeout(workerIdle),
		redowl.WithPollInterval(poll),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	// Publish work across many queues.
	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("q_%d", i)
		q := mustNewQueue(t, c, qName, redowl.WithPrefix(prefix))
		for j := 1; j <= msgsPerQ; j++ {
			_, err := q.Send(ctx, []byte(strconv.Itoa(j)), nil)
			require.NoError(t, err)
		}
	}

	expectedTotal := int64(queueCount * msgsPerQ)
	requireEventually(t, 8*time.Second, 20*time.Millisecond, func() bool {
		if pool.LiveWorkers() > maxWorkers {
			return false
		}
		return atomic.LoadInt64(&processed) >= expectedTotal
	}, "expected all messages processed")

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < queueCount; i++ {
		qn := fmt.Sprintf("q_%d", i)
		got := byQueue[qn]
		require.Len(t, got, msgsPerQ)
		for j := 1; j <= msgsPerQ; j++ {
			require.Equal(t, strconv.Itoa(j), got[j-1], "per-queue order should be preserved")
		}
	}
}

// TestWorkerPool_StatsTracksProcessedCounts verifies Stats() accounting.
//
// WorkerPool tracks per-queue processed message counts (best-effort; updated after each receive).
// The QueueIdleTimeout is intentionally small so a small worker set rotates across multiple queues
// instead of camping on the first few for the default 5 minutes.
func TestWorkerPool_StatsTracksProcessedCounts(t *testing.T) {
	_, c := newTestRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := "wps_" + strconv.FormatInt(time.Now().UnixNano(), 10)

	const (
		queueCount = 5
		msgsPerQ   = 2
	)

	var processed int64

	pool := redowl.NewWorkerPool(
		c,
		prefix,
		func(ctx context.Context, queueName string, msg *redowl.Message) error {
			atomic.AddInt64(&processed, 1)
			return nil
		},
		redowl.WithMaxWorkers(2),
		redowl.WithQueueIdleTimeout(200*time.Millisecond),
		redowl.WithPollInterval(20*time.Millisecond),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("stats_%d", i)
		q := mustNewQueue(t, c, qName, redowl.WithPrefix(prefix))
		for j := 0; j < msgsPerQ; j++ {
			_, err := q.Send(ctx, []byte("x"), nil)
			require.NoError(t, err)
		}
	}

	expectedTotal := int64(queueCount * msgsPerQ)
	requireEventually(t, 8*time.Second, 20*time.Millisecond, func() bool {
		return atomic.LoadInt64(&processed) >= expectedTotal
	}, "expected all messages processed")

	stats := pool.Stats()
	// WorkerPool tracks queues it has seen events for; by the time all messages are processed,
	// these queues should be present.
	require.GreaterOrEqual(t, len(stats), queueCount)
	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("stats_%d", i)
		require.GreaterOrEqual(t, stats[qName], int64(msgsPerQ))
	}
}

// ---------------------------------
// Redis cluster integration tests (optional)
// ---------------------------------

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

	startedMu sync.Mutex
	started   map[string]bool

	doneFlag int32
	doneCh   chan struct{}

	starterDone chan struct{}
	wg          sync.WaitGroup
}

func newDynamicConsumer(
	ctx context.Context,
	t *testing.T,
	consumerClient *redis.ClusterClient,
	prefix string,
	idlePoll time.Duration,
	idleExitStreak int,
	processDelay time.Duration,
	isInterestingQueue func(string) bool,
) *dynamicConsumer {
	t.Helper()

	pubsub := consumerClient.Subscribe(ctx, prefix+":events")
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { _ = pubsub.Close() })

	c := &dynamicConsumer{
		gotByKey:    map[string][]string{},
		started:    map[string]bool{},
		doneCh:      make(chan struct{}),
		starterDone: make(chan struct{}),
	}

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

		q := mustNewQueue(t, consumerClient, queueName,
			redowl.WithPrefix(prefix),
			redowl.WithVisibilityTimeout(5*time.Second),
		)

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
		pq := mustNewQueue(t, producerClient, qName,
			redowl.WithPrefix(prefix),
			redowl.WithVisibilityTimeout(5*time.Second),
		)
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
		require.Equal(t, expect, gotByKey[game], "queue %s order mismatch", game)
	}
}

func TestIntegration_Cluster_ProducerAndConsumerSeparated(t *testing.T) {
	// This is an optional integration test that requires a real Redis Cluster.
	// It is skipped unless REDIS_CLUSTER_ADDRS is set.
	//
	// Scenario:
	// - Producer sends ordered payloads to Game1..GameN (A1.., B1.., ...).
	// - Consumer subscribes to the namespace event channel and dynamically starts a per-queue worker.
	// Assertions:
	// - Per-queue order is preserved.
	// - Some cross-queue concurrency occurs.
	addrs := redisClusterAddrsFromEnv(t)
	producerClient, consumerClient := newClusterClients(t, addrs)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := newIntegrationPrefix()

	const (
		gameCount      = 14
		perGameMsgs    = 5
		idlePoll       = 200 * time.Millisecond
		idleExitStreak = 5
		processDelay   = 30 * time.Millisecond
	)

	consumer := newDynamicConsumer(ctx, t, consumerClient, prefix, idlePoll, idleExitStreak, processDelay, func(queue string) bool {
		return strings.HasPrefix(queue, "Game")
	})

	runProducer(ctx, t, producerClient, prefix, gameCount, perGameMsgs)
	publishProducerDone(ctx, t, producerClient, prefix)

	select {
	case <-consumer.starterDone:
	case <-ctx.Done():
		t.Fatal("timeout waiting for consumer starter")
	}

	consumer.wg.Wait()

	consumer.gotMu.Lock()
	gotCopy := make(map[string][]string, len(consumer.gotByKey))
	for k, v := range consumer.gotByKey {
		gotCopy[k] = append([]string(nil), v...)
	}
	consumer.gotMu.Unlock()

	assertOrderedPerGame(t, gotCopy, gameCount, perGameMsgs)
	require.Greater(t, atomic.LoadInt64(&consumer.maxInflight), int64(1), "expected some cross-queue concurrency")
}

func TestIntegration_Cluster_WorkerPoolManyQueues(t *testing.T) {
	// This is an optional integration test that requires a real Redis Cluster.
	// It is skipped unless REDIS_CLUSTER_ADDRS is set.
	//
	// Scenario:
	// - Create many queues and send messages.
	// - WorkerPool should discover and process all queues using limited workers.
	addrs := redisClusterAddrsFromEnv(t)
	producerClient, consumerClient := newClusterClients(t, addrs)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := newIntegrationPrefix()

	const (
		queueCount  = 100
		workerCount = 10
		msgsPerQ    = 5
	)

	var processed int64
	processedByQueue := make(map[string]int64)
	var mu sync.Mutex

	pool := redowl.NewWorkerPool(
		consumerClient,
		prefix,
		func(ctx context.Context, queueName string, msg *redowl.Message) error {
			atomic.AddInt64(&processed, 1)
			mu.Lock()
			processedByQueue[queueName]++
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			return nil
		},
		redowl.WithMaxWorkers(workerCount),
		redowl.WithQueueIdleTimeout(2*time.Minute),
		redowl.WithPollInterval(200*time.Millisecond),
	)

	require.NoError(t, pool.Start(ctx))
	defer pool.Stop()

	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("queue_%d", i)
		q := mustNewQueue(t, producerClient, qName,
			redowl.WithPrefix(prefix),
			redowl.WithVisibilityTimeout(5*time.Second),
		)
		for j := 0; j < msgsPerQ; j++ {
			_, err := q.Send(ctx, []byte(fmt.Sprintf("msg_%d", j)), nil)
			require.NoError(t, err)
		}
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processed) >= int64(queueCount*msgsPerQ) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	finalProcessed := atomic.LoadInt64(&processed)
	require.Equal(t, int64(queueCount*msgsPerQ), finalProcessed, "should process all messages with worker pool")

	mu.Lock()
	require.Len(t, processedByQueue, queueCount, "should process messages from all queues")
	mu.Unlock()

	stats := pool.Stats()
	_ = stats
}
