package redowl

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type Options struct {
	Prefix            string
	VisibilityTimeout time.Duration
	MaxReceiveCount   int64
	ReaperInterval    time.Duration
	TriggerClient     redis.UniversalClient
}

type Option func(*Options)

func WithPrefix(prefix string) Option {
	return func(o *Options) { o.Prefix = prefix }
}

func WithVisibilityTimeout(d time.Duration) Option {
	return func(o *Options) { o.VisibilityTimeout = d }
}

// WithMaxReceiveCount sets how many times a message may be received
// before being moved to DLQ. 0 disables DLQ behavior.
func WithMaxReceiveCount(n int64) Option {
	return func(o *Options) { o.MaxReceiveCount = n }
}

// WithReaperInterval enables a background reaper moving expired
// in-flight messages back to the ready list.
// Set to 0 to disable the background reaper (you can still call RequeueExpiredOnce).
func WithReaperInterval(d time.Duration) Option {
	return func(o *Options) { o.ReaperInterval = d }
}

// WithTriggerClient enables PubSub triggers.
// It must be a go-redis/v9 client that supports Subscribe (e.g. *redis.Client or *redis.ClusterClient).
func WithTriggerClient(c redis.UniversalClient) Option {
	return func(o *Options) { o.TriggerClient = c }
}
