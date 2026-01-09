package redowl

import "errors"

var (
	ErrInvalidQueueName      = errors.New("redowl: invalid queue name")
	ErrInvalidReceiptHandle  = errors.New("redowl: invalid receipt handle")
	ErrTriggersNotConfigured = errors.New("redowl: triggers not configured")
)
