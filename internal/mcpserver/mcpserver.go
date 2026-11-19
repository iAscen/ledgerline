// Package mcpserver exposes the extraction service to AI agents over the
// Model Context Protocol (streamable HTTP transport), using the official Go
// MCP SDK. Tools call the same application service as the REST API, so
// validation, idempotency and tenant isolation are identical on both paths.
package mcpserver

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/iAscen/ledgerline/internal/auth"
	"github.com/iAscen/ledgerline/internal/domain"
	"github.com/iAscen/ledgerline/internal/jobs"
)

// ExtractInput is the extract_receipt tool input.
type ExtractInput struct {
	DocumentBase64 string `json:"document_base64" jsonschema:"the receipt image or PDF, base64-encoded"`
	MediaType      string `json:"media_type" jsonschema:"one of image/jpeg, image/png, image/gif, image/webp, application/pdf"`
	IdempotencyKey string `json:"idempotency_key,omitempty" jsonschema:"optional; defaults to a hash of the document so agent retries never duplicate work"`
	WaitSeconds    int    `json:"wait_seconds,omitempty" jsonschema:"optional; wait up to this many seconds (max 60) for the result before returning"`
}

// GetJobInput is the get_job tool input.
type GetJobInput struct {
	JobID string `json:"job_id" jsonschema:"the job id returned by extract_receipt"`
}

// ListJobsInput is the list_jobs tool input.
type ListJobsInput struct {
	Limit int `json:"limit,omitempty" jsonschema:"maximum number of jobs to return (1-100, default 10)"`
}

// NewHandler returns the HTTP handler for /mcp. A server is built per
// request, bound to the authenticated tenant, and run statelessly so any
// replica can answer any request (no sticky sessions behind the K8s Service).
func NewHandler(svc *jobs.Service, version string) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server {
		p, _ := auth.FromContext(r.Context())
		return newServer(svc, version, p)
	}, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
}

func newServer(svc *jobs.Service, version string, p auth.Principal) *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "ledgerline", Version: version}, nil)

	mcp.AddTool(s, &mcp.Tool{
		Name:        "extract_receipt",
		Description: "Extract structured data (merchant, date, taxes, totals, line items) from a receipt or invoice image/PDF. Money amounts are integers in cents.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ExtractInput) (*mcp.CallToolResult, any, error) {
		if !p.Has(auth.ScopeJobsWrite) {
			return toolError("token lacks scope " + auth.ScopeJobsWrite), nil, nil
		}
		doc, err := base64.StdEncoding.DecodeString(in.DocumentBase64)
		if err != nil {
			return toolError("document_base64 is not valid base64"), nil, nil
		}
		key := in.IdempotencyKey
		if key == "" {
			sum := sha256.Sum256(doc)
			key = "mcp-" + hex.EncodeToString(sum[:16])
		}
		job, _, err := svc.Create(ctx, p.Tenant, jobs.CreateInput{IdempotencyKey: key, MediaType: in.MediaType, Document: doc})
		if err != nil {
			if jobs.IsValidation(err) || errors.Is(err, domain.ErrIdempotencyConflict) {
				return toolError(err.Error()), nil, nil
			}
			return nil, nil, err
		}
		if wait := min(max(in.WaitSeconds, 0), 60); wait > 0 {
			job = waitFor(ctx, svc, p.Tenant, job, time.Duration(wait)*time.Second)
		}
		return jsonResult(job)
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "get_job",
		Description: "Get an extraction job's status and, when it has succeeded, the extracted receipt.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in GetJobInput) (*mcp.CallToolResult, any, error) {
		job, err := svc.Get(ctx, p.Tenant, in.JobID)
		if errors.Is(err, domain.ErrNotFound) {
			return toolError("job not found"), nil, nil
		}
		if err != nil {
			return nil, nil, err
		}
		return jsonResult(job)
	})

	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_jobs",
		Description: "List the caller's most recent extraction jobs, newest first.",
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListJobsInput) (*mcp.CallToolResult, any, error) {
		limit := in.Limit
		if limit == 0 {
			limit = 10
		}
		list, err := svc.List(ctx, p.Tenant, limit)
		if jobs.IsValidation(err) {
			return toolError(err.Error()), nil, nil
		}
		if err != nil {
			return nil, nil, err
		}
		return jsonResult(map[string]any{"jobs": list})
	})
	return s
}

func waitFor(ctx context.Context, svc *jobs.Service, tenant string, job domain.Job, d time.Duration) domain.Job {
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for !job.Status.Terminal() {
		select {
		case <-ctx.Done():
			return job
		case <-t.C:
			if j, err := svc.Get(ctx, tenant, job.ID.String()); err == nil {
				job = j
			}
		}
	}
	return job
}

func jsonResult(v any) (*mcp.CallToolResult, any, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, nil, fmt.Errorf("encode result: %w", err)
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}}, nil, nil
}

func toolError(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: msg}}}
}
