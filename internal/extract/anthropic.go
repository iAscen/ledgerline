package extract

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go/v2"
	"github.com/anthropics/anthropic-sdk-go/v2/option"

	"github.com/iAscen/ledgerline/internal/domain"
)

// DefaultModel is the Claude model used for extraction unless overridden.
const DefaultModel = "claude-opus-5-5"

const systemPrompt = `You extract structured data from photos and scans of purchase receipts and invoices.
Return only the fields defined by the output schema.
All money amounts are integers in minor units (cents): $12.34 becomes 1234.
List each tax line separately using the name printed on the receipt (for Canadian receipts, e.g. GST/TPS, QST/TVQ, HST).
Use an ISO 4217 currency code and an ISO 8601 date (YYYY-MM-DD). Use an empty string for a field that is not present.
Set confidence between 0 and 1 to reflect how legible and complete the document was.
If the document is not a receipt or invoice, set is_receipt to false.`

// receiptSchema is the JSON Schema the model's output is constrained to.
var receiptSchema = map[string]any{
	"type":                 "object",
	"additionalProperties": false,
	"required": []string{"is_receipt", "merchant", "merchant_tax_id", "date", "currency", "subtotal_minor", "taxes",
		"total_minor", "payment_method", "category", "line_items", "confidence"},
	"properties": map[string]any{
		"is_receipt":      map[string]any{"type": "boolean"},
		"merchant":        map[string]any{"type": "string"},
		"merchant_tax_id": map[string]any{"type": "string"},
		"date":            map[string]any{"type": "string"},
		"currency":        map[string]any{"type": "string"},
		"subtotal_minor":  map[string]any{"type": "integer"},
		"total_minor":     map[string]any{"type": "integer"},
		"payment_method":  map[string]any{"type": "string"},
		"category": map[string]any{"type": "string", "enum": []string{
			"meals", "groceries", "travel", "lodging", "fuel", "office_supplies", "software", "utilities", "other",
		}},
		"confidence": map[string]any{"type": "number"},
		"taxes": map[string]any{"type": "array", "items": map[string]any{
			"type": "object", "additionalProperties": false, "required": []string{"name", "amount_minor"},
			"properties": map[string]any{"name": map[string]any{"type": "string"}, "amount_minor": map[string]any{"type": "integer"}},
		}},
		"line_items": map[string]any{"type": "array", "items": map[string]any{
			"type": "object", "additionalProperties": false, "required": []string{"description", "quantity", "amount_minor"},
			"properties": map[string]any{
				"description":  map[string]any{"type": "string"},
				"quantity":     map[string]any{"type": "number"},
				"amount_minor": map[string]any{"type": "integer"},
			},
		}},
	},
}

type modelOutput struct {
	IsReceipt bool `json:"is_receipt"`
	domain.Receipt
}

// AnthropicConfig configures the Claude provider.
type AnthropicConfig struct {
	APIKey  string
	Model   string
	Effort  string // optional: low|medium|high|xhigh|max
	Timeout time.Duration
	BaseURL string // optional, for tests
}

// Anthropic extracts receipts with Claude vision.
type Anthropic struct {
	client anthropic.Client
	cfg    AnthropicConfig
}

// NewAnthropic builds the provider. Retries are disabled in the SDK on
// purpose: the job queue owns retry policy, and hiding failures inside the
// client would keep the circuit breaker from seeing an outage.
func NewAnthropic(cfg AnthropicConfig) *Anthropic {
	if cfg.Model == "" {
		cfg.Model = DefaultModel
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 2 * time.Minute
	}
	opts := []option.RequestOption{
		option.WithAPIKey(cfg.APIKey),
		option.WithMaxRetries(0),
		option.WithRequestTimeout(cfg.Timeout),
	}
	if cfg.BaseURL != "" {
		opts = append(opts, option.WithBaseURL(cfg.BaseURL))
	}
	return &Anthropic{client: anthropic.NewClient(opts...), cfg: cfg}
}

// Name implements Provider.
func (a *Anthropic) Name() string { return "anthropic" }

// Extract implements Provider.
func (a *Anthropic) Extract(ctx context.Context, doc Document) (domain.Receipt, error) {
	data := base64.StdEncoding.EncodeToString(doc.Data)
	var block anthropic.BetaContentBlockParamUnion
	switch doc.MediaType {
	case "application/pdf":
		block = anthropic.NewBetaDocumentBlock(anthropic.BetaBase64PDFSourceParam{Data: data})
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		block = anthropic.NewBetaImageBlock(anthropic.BetaBase64ImageSourceParam{
			Data: data, MediaType: anthropic.BetaBase64ImageSourceMediaType(doc.MediaType),
		})
	default:
		return domain.Receipt{}, Permanent("unsupported media type %q", doc.MediaType)
	}

	params := anthropic.BetaMessageNewParams{
		Model:     a.cfg.Model,
		MaxTokens: 16000,
		System:    []anthropic.BetaTextBlockParam{{Text: systemPrompt}},
		Messages: []anthropic.BetaMessageParam{
			anthropic.NewBetaUserMessage(block, anthropic.NewBetaTextBlock("Extract this receipt.")),
		},
		OutputConfig: anthropic.BetaOutputConfigParam{
			Format: anthropic.BetaJSONOutputFormatParam{Schema: receiptSchema},
			Effort: anthropic.BetaOutputConfigEffort(a.cfg.Effort),
		},
		// If a safety classifier declines, let the API re-serve the request on
		// a fallback model instead of failing the job.
		Betas:     []anthropic.AnthropicBeta{anthropic.AnthropicBetaServerSideFallback2026_07_01},
		Fallbacks: anthropic.BetaFallbacksParamOfDefault(),
	}

	msg, err := a.client.Beta.Messages.New(ctx, params)
	if err != nil {
		return domain.Receipt{}, classify(err)
	}
	switch msg.StopReason {
	case anthropic.BetaStopReasonRefusal:
		return domain.Receipt{}, Permanent("model declined the document")
	case anthropic.BetaStopReasonMaxTokens:
		return domain.Receipt{}, Permanent("output truncated at max_tokens")
	}

	var text strings.Builder
	for _, b := range msg.Content {
		if t, ok := b.AsAny().(anthropic.BetaTextBlock); ok {
			text.WriteString(t.Text)
		}
	}
	var out modelOutput
	if err := json.Unmarshal([]byte(text.String()), &out); err != nil {
		// Structured outputs make this unexpected; treat as transient so a
		// retry gets another sample.
		return domain.Receipt{}, fmt.Errorf("decode model output: %w", err)
	}
	if !out.IsReceipt {
		return domain.Receipt{}, Permanent("document is not a receipt")
	}
	r := out.Receipt
	r.Provider, r.Model = a.Name(), msg.Model
	if r.Taxes == nil {
		r.Taxes = []domain.Tax{}
	}
	if r.LineItems == nil {
		r.LineItems = []domain.LineItem{}
	}
	return r, nil
}

// classify maps API errors to permanent (caller's fault) or transient
// (provider's fault; retried and counted by the breaker).
func classify(err error) error {
	var apiErr *anthropic.Error
	if errors.As(err, &apiErr) {
		switch apiErr.StatusCode {
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
			return &PermanentError{Err: err}
		}
	}
	return err
}
