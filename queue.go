package redowl

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	cmd  redis.Cmdable
	opt  Options
	name string

	mu        sync.Mutex
	reaperCh  chan struct{}
	reaperWg  sync.WaitGroup
	reaperRun bool
}

func New(cmd redis.Cmdable, name string, opts ...Option) (*Queue, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, ErrInvalidQueueName
	}

	opt := Options{
		Prefix:            "redowl",
		VisibilityTimeout: 30 * time.Second,
		MaxReceiveCount:   0,
		ReaperInterval:    0,
		TriggerClient:     nil,
	}
	for _, fn := range opts {
		if fn != nil {
			fn(&opt)
		}
	}
	if opt.TriggerClient == nil {
		if uc, ok := cmd.(redis.UniversalClient); ok {
			opt.TriggerClient = uc
		}
	}
	if opt.Prefix == "" {
		opt.Prefix = "redowl"
	}
	if opt.VisibilityTimeout <= 0 {
		opt.VisibilityTimeout = 30 * time.Second
	}

	q := &Queue{cmd: cmd, opt: opt, name: name}
	if opt.ReaperInterval > 0 {
		q.StartReaper()
	}
	return q, nil
}

func (q *Queue) Name() string { return q.name }

func (q *Queue) readyKey() string { return q.opt.Prefix + ":" + q.name + ":ready" }
func (q *Queue) dlqKey() string   { return q.opt.Prefix + ":" + q.name + ":dlq" }
func (q *Queue) msgKey(id string) string {
	return q.opt.Prefix + ":" + q.name + ":msg:" + id
}
func (q *Queue) receiptMapKey() string { return q.opt.Prefix + ":" + q.name + ":receipt" }
func (q *Queue) inflightKey() string   { return q.opt.Prefix + ":" + q.name + ":inflight" } // zset: receipt -> visibleAtUnixMs
func (q *Queue) eventChannel() string  { return q.opt.Prefix + ":" + q.name + ":events" }

func (q *Queue) namespaceEventChannel() string {
	return q.opt.Prefix + ":events"
}

func (q *Queue) publish(ctx context.Context, ev Event) {
	if q.opt.TriggerClient == nil {
		return
	}
	b, _ := json.Marshal(ev)
	// Publish to both a per-queue channel and a prefix-level namespace channel.
	// This allows consumers to discover queues dynamically without relying on PSUBSCRIBE.
	_ = q.opt.TriggerClient.Publish(ctx, q.eventChannel(), b).Err()
	_ = q.opt.TriggerClient.Publish(ctx, q.namespaceEventChannel(), b).Err()
}

func randomHex(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (q *Queue) Send(ctx context.Context, body []byte, attrs map[string]string) (string, error) {
	id, err := randomHex(16)
	if err != nil {
		return "", err
	}
	createdAt := time.Now()
	attrJSON, _ := json.Marshal(attrs)

	msgKey := q.msgKey(id)
	pipe := q.cmd.Pipeline()
	pipe.HSet(ctx, msgKey,
		"body", base64.StdEncoding.EncodeToString(body),
		"attrs", string(attrJSON),
		"rc", int64(0),
		"created_at_ms", createdAt.UnixMilli(),
	)
	pipe.RPush(ctx, q.readyKey(), id)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return "", err
	}

	q.publish(ctx, Event{Type: EventSent, Queue: q.name, MessageID: id, AtUnixMs: time.Now().UnixMilli()})
	return id, nil
}

// Receive returns one message or (nil, nil) when queue is empty.
func (q *Queue) Receive(ctx context.Context) (*Message, error) {
	return q.ReceiveWithWait(ctx, 0)
}

// ReceiveDLQ returns one DLQ message or (nil, nil) when DLQ is empty.
// DLQ messages are returned with a ReceiptHandle so you can call Ack to delete.
func (q *Queue) ReceiveDLQ(ctx context.Context) (*Message, error) {
	return q.ReceiveDLQWithWait(ctx, 0)
}

// ReceiveDLQWithWait uses BLPOP when wait > 0.
// DLQ receive does not apply visibility timeout; messages are removed from the DLQ list when delivered.
// The returned ReceiptHandle can be used with Ack.
func (q *Queue) ReceiveDLQWithWait(ctx context.Context, wait time.Duration) (*Message, error) {
	const maxSkips = 5
	first := true
	for skips := 0; skips < maxSkips; skips++ {
		var id string
		if wait > 0 && first {
			first = false
			res, err := q.cmd.BLPop(ctx, wait, q.dlqKey()).Result()
			if err != nil {
				if err == redis.Nil {
					return nil, nil
				}
				return nil, err
			}
			if len(res) == 2 {
				id = res[1]
			}
		} else {
			s, err := q.cmd.LPop(ctx, q.dlqKey()).Result()
			if err != nil {
				if err == redis.Nil {
					return nil, nil
				}
				return nil, err
			}
			id = s
		}
		if id == "" {
			return nil, nil
		}

		msgKey := q.msgKey(id)
		exists, err := q.cmd.Exists(ctx, msgKey).Result()
		if err != nil {
			return nil, err
		}
		if exists == 0 {
			continue
		}

		receipt, err := randomHex(16)
		if err != nil {
			return nil, err
		}
		pipe := q.cmd.Pipeline()
		pipe.HSet(ctx, q.receiptMapKey(), receipt, id)
		vals := pipe.HMGet(ctx, msgKey, "body", "attrs", "rc", "created_at_ms")
		_, err = pipe.Exec(ctx)
		if err != nil {
			return nil, err
		}

		v, err := vals.Result()
		if err != nil {
			return nil, err
		}

		var bodyB64 string
		if len(v) > 0 {
			if s, ok := v[0].(string); ok {
				bodyB64 = s
			}
		}
		body, _ := base64.StdEncoding.DecodeString(bodyB64)

		attrs := map[string]string{}
		if len(v) > 1 {
			if s, ok := v[1].(string); ok && s != "" {
				_ = json.Unmarshal([]byte(s), &attrs)
			}
		}

		rc := int64(0)
		if len(v) > 2 {
			switch t := v[2].(type) {
			case string:
				if n, perr := parseInt64(t); perr == nil {
					rc = n
				}
			case int64:
				rc = t
			case float64:
				rc = int64(t)
			}
		}

		createdAt := time.Time{}
		if len(v) > 3 {
			switch t := v[3].(type) {
			case string:
				if ms, perr := parseInt64(t); perr == nil {
					createdAt = time.UnixMilli(ms)
				}
			case int64:
				createdAt = time.UnixMilli(t)
			case float64:
				createdAt = time.UnixMilli(int64(t))
			}
		}

		msg := &Message{
			ID:            id,
			Body:          body,
			Attributes:    attrs,
			ReceiptHandle: receipt,
			ReceiveCount:  rc,
			VisibleAt:     time.Now(),
			CreatedAt:     createdAt,
		}

		q.publish(ctx, Event{Type: EventDLQReceived, Queue: q.name, MessageID: id, AtUnixMs: time.Now().UnixMilli(), Extra: map[string]string{"rc": strconv.FormatInt(rc, 10)}})
		return msg, nil
	}

	return nil, nil
}

// RedriveDLQ moves up to n messages from DLQ back to the ready queue.
// Returns how many messages were moved.
//
// Notes:
// - This only moves message IDs (the message body remains in the msg hash).
// - Orphaned message IDs (msg hash missing) will be discarded (removed from ready) to avoid poisoning the queue.
func (q *Queue) RedriveDLQ(ctx context.Context, n int64) (int, error) {
	if n <= 0 {
		return 0, nil
	}

	moved := 0
	for i := int64(0); i < n; i++ {
		id, err := q.cmd.RPopLPush(ctx, q.dlqKey(), q.readyKey()).Result()
		if err != nil {
			if err == redis.Nil {
				return moved, nil
			}
			return moved, err
		}
		if id == "" {
			return moved, nil
		}

		exists, err := q.cmd.Exists(ctx, q.msgKey(id)).Result()
		if err != nil {
			return moved, err
		}
		if exists == 0 {
			// best-effort cleanup
			_ = q.cmd.LRem(ctx, q.readyKey(), 1, id).Err()
			continue
		}

		// Reset receive count on redrive so it can be processed again.
		if err := q.cmd.HSet(ctx, q.msgKey(id), "rc", int64(0)).Err(); err != nil {
			return moved, err
		}

		moved++
		q.publish(ctx, Event{Type: EventDLQRedriven, Queue: q.name, MessageID: id, AtUnixMs: time.Now().UnixMilli(), Extra: map[string]string{"rc": "0"}})
	}

	return moved, nil
}

// ReceiveWithWait uses BLPOP when wait >= 1s (Redis BLPOP is seconds-resolution).
// For sub-second waits, it polls with LPOP + small sleeps up to the deadline.
func (q *Queue) ReceiveWithWait(ctx context.Context, wait time.Duration) (*Message, error) {
	// Best-effort cleanup of expired in-flight receipts.
	_, _ = q.RequeueExpiredOnce(ctx, 100)

	// Skip orphaned IDs (e.g. message hash deleted but ID still present in ready list).
	// This can happen in rare race conditions or manual operations.
	const maxSkips = 5
	first := true
	for skips := 0; skips < maxSkips; skips++ {
		var id string
		if wait > 0 && first {
			first = false
			if wait >= time.Second {
				// BLPOP takes integer seconds. Round up to avoid truncation warnings.
				waitSec := ((wait + time.Second - 1) / time.Second) * time.Second
				res, err := q.cmd.BLPop(ctx, waitSec, q.readyKey()).Result()
				if err != nil {
					if err == redis.Nil {
						return nil, nil
					}
					return nil, err
				}
				if len(res) == 2 {
					id = res[1]
				}
			} else {
				deadline := time.Now().Add(wait)
				for {
					s, err := q.cmd.LPop(ctx, q.readyKey()).Result()
					if err == nil {
						id = s
						break
					}
					if err != redis.Nil {
						return nil, err
					}
					if time.Now().After(deadline) {
						return nil, nil
					}

					// Sleep a bit to avoid hot-looping.
					remaining := time.Until(deadline)
					sleep := 10 * time.Millisecond
					if remaining < sleep {
						sleep = remaining
					}
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(sleep):
					}
				}
			}
		} else {
			s, err := q.cmd.LPop(ctx, q.readyKey()).Result()
			if err != nil {
				if err == redis.Nil {
					return nil, nil
				}
				return nil, err
			}
			id = s
		}
		if id == "" {
			return nil, nil
		}

		msgKey := q.msgKey(id)
		exists, err := q.cmd.Exists(ctx, msgKey).Result()
		if err != nil {
			return nil, err
		}
		if exists == 0 {
			continue
		}

		// From here on, proceed with a normal receive.
		rc, err := q.cmd.HIncrBy(ctx, msgKey, "rc", 1).Result()
		if err != nil {
			return nil, err
		}

		if q.opt.MaxReceiveCount > 0 && rc > q.opt.MaxReceiveCount {
			pipe := q.cmd.Pipeline()
			pipe.RPush(ctx, q.dlqKey(), id)
			_, err := pipe.Exec(ctx)
			if err != nil {
				return nil, err
			}
			q.publish(ctx, Event{Type: EventToDLQ, Queue: q.name, MessageID: id, AtUnixMs: time.Now().UnixMilli(), Extra: map[string]string{"rc": strconv.FormatInt(rc, 10)}})
			return nil, nil
		}

		receipt, err := randomHex(16)
		if err != nil {
			return nil, err
		}
		visibleAt := time.Now().Add(q.opt.VisibilityTimeout)

		pipe := q.cmd.Pipeline()
		pipe.HSet(ctx, q.receiptMapKey(), receipt, id)
		pipe.ZAdd(ctx, q.inflightKey(), redis.Z{Score: float64(visibleAt.UnixMilli()), Member: receipt})
		vals := pipe.HMGet(ctx, msgKey, "body", "attrs", "created_at_ms")
		_, err = pipe.Exec(ctx)
		if err != nil {
			return nil, err
		}

		v, err := vals.Result()
		if err != nil {
			return nil, err
		}

		var bodyB64 string
		if len(v) > 0 {
			if s, ok := v[0].(string); ok {
				bodyB64 = s
			}
		}
		body, _ := base64.StdEncoding.DecodeString(bodyB64)

		attrs := map[string]string{}
		if len(v) > 1 {
			if s, ok := v[1].(string); ok && s != "" {
				_ = json.Unmarshal([]byte(s), &attrs)
			}
		}

		createdAt := time.Time{}
		if len(v) > 2 {
			switch t := v[2].(type) {
			case string:
				if ms, perr := parseInt64(t); perr == nil {
					createdAt = time.UnixMilli(ms)
				}
			case int64:
				createdAt = time.UnixMilli(t)
			case float64:
				createdAt = time.UnixMilli(int64(t))
			}
		}

		msg := &Message{
			ID:            id,
			Body:          body,
			Attributes:    attrs,
			ReceiptHandle: receipt,
			ReceiveCount:  rc,
			VisibleAt:     visibleAt,
			CreatedAt:     createdAt,
		}

		q.publish(ctx, Event{Type: EventReceived, Queue: q.name, MessageID: id, AtUnixMs: time.Now().UnixMilli(), Extra: map[string]string{"rc": strconv.FormatInt(rc, 10)}})
		return msg, nil
	}

	return nil, nil
}

func (q *Queue) Ack(ctx context.Context, receiptHandle string) error {
	if strings.TrimSpace(receiptHandle) == "" {
		return ErrInvalidReceiptHandle
	}
	id, err := q.cmd.HGet(ctx, q.receiptMapKey(), receiptHandle).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrInvalidReceiptHandle
		}
		return err
	}

	pipe := q.cmd.Pipeline()
	pipe.HDel(ctx, q.receiptMapKey(), receiptHandle)
	pipe.ZRem(ctx, q.inflightKey(), receiptHandle)
	pipe.Del(ctx, q.msgKey(id))
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}
	q.publish(ctx, Event{Type: EventAcked, Queue: q.name, MessageID: id, AtUnixMs: time.Now().UnixMilli()})
	return nil
}

func (q *Queue) ChangeVisibility(ctx context.Context, receiptHandle string, d time.Duration) error {
	if strings.TrimSpace(receiptHandle) == "" {
		return ErrInvalidReceiptHandle
	}
	_, err := q.cmd.HGet(ctx, q.receiptMapKey(), receiptHandle).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrInvalidReceiptHandle
		}
		return err
	}
	visibleAt := time.Now().Add(d)
	return q.cmd.ZAdd(ctx, q.inflightKey(), redis.Z{Score: float64(visibleAt.UnixMilli()), Member: receiptHandle}).Err()
}

// RequeueExpiredOnce moves expired in-flight messages back to the ready list.
// Returns how many receipts were requeued.
func (q *Queue) RequeueExpiredOnce(ctx context.Context, batch int64) (int, error) {
	if batch <= 0 {
		batch = 100
	}
	now := time.Now().UnixMilli()
	receipts, err := q.cmd.ZRangeByScore(ctx, q.inflightKey(), &redis.ZRangeBy{Min: "-inf", Max: strconv.FormatInt(now, 10), Offset: 0, Count: batch}).Result()
	if err != nil {
		return 0, err
	}
	if len(receipts) == 0 {
		return 0, nil
	}

	requeued := 0
	for _, rh := range receipts {
		id, err := q.cmd.HGet(ctx, q.receiptMapKey(), rh).Result()
		if err != nil {
			_ = q.cmd.ZRem(ctx, q.inflightKey(), rh).Err()
			_ = q.cmd.HDel(ctx, q.receiptMapKey(), rh).Err()
			continue
		}
		exists, err := q.cmd.Exists(ctx, q.msgKey(id)).Result()
		if err != nil {
			return requeued, err
		}
		if exists == 0 {
			_ = q.cmd.ZRem(ctx, q.inflightKey(), rh).Err()
			_ = q.cmd.HDel(ctx, q.receiptMapKey(), rh).Err()
			continue
		}
		pipe := q.cmd.Pipeline()
		pipe.LPush(ctx, q.readyKey(), id)
		pipe.ZRem(ctx, q.inflightKey(), rh)
		pipe.HDel(ctx, q.receiptMapKey(), rh)
		_, err = pipe.Exec(ctx)
		if err != nil {
			return requeued, err
		}
		requeued++
		q.publish(ctx, Event{Type: EventRequeued, Queue: q.name, MessageID: id, AtUnixMs: time.Now().UnixMilli()})
	}
	return requeued, nil
}

func (q *Queue) StartReaper() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.reaperRun {
		return
	}
	if q.opt.ReaperInterval <= 0 {
		return
	}
	q.reaperRun = true
	q.reaperCh = make(chan struct{})
	q.reaperWg.Add(1)
	go func() {
		defer q.reaperWg.Done()
		t := time.NewTicker(q.opt.ReaperInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				ctx := context.Background()
				_, _ = q.RequeueExpiredOnce(ctx, 500)
			case <-q.reaperCh:
				return
			}
		}
	}()
}

func (q *Queue) StopReaper() {
	q.mu.Lock()
	ch := q.reaperCh
	running := q.reaperRun
	q.reaperCh = nil
	q.reaperRun = false
	q.mu.Unlock()

	if running && ch != nil {
		close(ch)
		q.reaperWg.Wait()
	}
}

// Subscribe registers a handler receiving PubSub events.
// Requires WithTriggerClient.
func (q *Queue) Subscribe(ctx context.Context, handler func(Event)) (func() error, error) {
	if q.opt.TriggerClient == nil {
		return nil, ErrTriggersNotConfigured
	}
	pubsub := q.opt.TriggerClient.Subscribe(ctx, q.eventChannel())
	ch := pubsub.Channel()

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var ev Event
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err == nil {
					handler(ev)
				}
			}
		}
	}()

	return func() error {
		close(stop)
		return pubsub.Close()
	}, nil
}

func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(s), 10, 64)
}
