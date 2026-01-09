package redowl

import "time"

// Message is a SQS-like message.
// ReceiptHandle is required for Ack.
type Message struct {
	ID            string
	Body          []byte
	Attributes    map[string]string
	ReceiptHandle string
	ReceiveCount  int64
	VisibleAt     time.Time
	CreatedAt     time.Time
}

type EventType string

const (
	EventSent        EventType = "sent"
	EventReceived    EventType = "received"
	EventDLQReceived EventType = "dlq_received"
	EventDLQRedriven EventType = "dlq_redriven"
	EventRequeued    EventType = "requeued"
	EventToDLQ       EventType = "to_dlq"
	EventAcked       EventType = "acked"
)

type Event struct {
	Type      EventType         `json:"type"`
	Queue     string            `json:"queue"`
	MessageID string            `json:"message_id"`
	AtUnixMs  int64             `json:"at_unix_ms"`
	Extra     map[string]string `json:"extra,omitempty"`
}
