// Package redowl provides a small SQS-like queue built on Redis.
//
// It uses:
// - Redis List for ready/DLQ
// - Redis Hash for message metadata and receipt mapping
// - Redis ZSet to track visibility timeout deadlines
// - Redis PubSub (optional) for triggers/events
package redowl
