// Package domain holds the core types shared by the API, the worker and the
// storage layer. It has no dependencies on transport or persistence.
package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Status is the lifecycle state of an extraction job.
type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusSucceeded  Status = "succeeded"
	StatusFailed     Status = "failed"
)

// Terminal reports whether no further processing will happen.
func (s Status) Terminal() bool { return s == StatusSucceeded || s == StatusFailed }

// SupportedMediaTypes is the set Claude vision accepts, plus PDF.
var SupportedMediaTypes = map[string]bool{
	"image/jpeg":      true,
	"image/png":       true,
	"image/gif":       true,
	"image/webp":      true,
	"application/pdf": true,
}

// Job is an asynchronous extraction request.
type Job struct {
	ID             uuid.UUID  `json:"id"`
	Tenant         string     `json:"-"`
	IdempotencyKey string     `json:"idempotency_key"`
	RequestHash    string     `json:"-"`
	Status         Status     `json:"status"`
	MediaType      string     `json:"media_type"`
	WebhookURL     string     `json:"webhook_url,omitempty"`
	WebhookStatus  string     `json:"webhook_status,omitempty"`
	TraceParent    string     `json:"-"`
	Result         *Receipt   `json:"result,omitempty"`
	Error          string     `json:"error,omitempty"`
	Attempts       int        `json:"attempts"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`

	// Document is only populated on a claimed job (worker side).
	Document []byte `json:"-"`
}

// NewJob is the input to Store.CreateJob.
type NewJob struct {
	ID             uuid.UUID
	Tenant         string
	IdempotencyKey string
	RequestHash    string
	MediaType      string
	Document       []byte
	WebhookURL     string
	TraceParent    string
}

// Receipt is the structured output of an extraction. Money is stored in minor
// units (cents) as integers: floats and money do not mix.
type Receipt struct {
	Merchant      string     `json:"merchant"`
	MerchantTaxID string     `json:"merchant_tax_id,omitempty"`
	Date          string     `json:"date,omitempty"` // ISO 8601 (YYYY-MM-DD)
	Currency      string     `json:"currency"`       // ISO 4217
	SubtotalMinor int64      `json:"subtotal_minor"`
	Taxes         []Tax      `json:"taxes"`
	TotalMinor    int64      `json:"total_minor"`
	PaymentMethod string     `json:"payment_method,omitempty"`
	Category      string     `json:"category,omitempty"`
	LineItems     []LineItem `json:"line_items"`
	Confidence    float64    `json:"confidence"` // 0..1, model self-assessed
	Provider      string     `json:"provider"`
	Model         string     `json:"model,omitempty"`
}

// Tax is one tax line (e.g. GST/TPS and QST/TVQ on a Québec receipt).
type Tax struct {
	Name        string `json:"name"`
	AmountMinor int64  `json:"amount_minor"`
}

// LineItem is one purchased item.
type LineItem struct {
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	AmountMinor int64   `json:"amount_minor"`
}

// Sentinel errors shared across layers.
var (
	ErrNotFound            = errors.New("not found")
	ErrNoJobAvailable      = errors.New("no job available")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request body")
)
