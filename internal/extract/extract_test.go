package extract

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFakeIsDeterministicAndBalanced(t *testing.T) {
	f := NewFake(0, 0)
	doc := Document{MediaType: "image/png", Data: []byte("receipt-bytes")}
	a, err := f.Extract(context.Background(), doc)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := f.Extract(context.Background(), doc)
	if a.Merchant != b.Merchant || a.TotalMinor != b.TotalMinor {
		t.Fatal("same document must give the same receipt")
	}
	var taxes int64
	for _, tx := range a.Taxes {
		taxes += tx.AmountMinor
	}
	if a.SubtotalMinor+taxes != a.TotalMinor {
		t.Fatalf("subtotal %d + taxes %d != total %d", a.SubtotalMinor, taxes, a.TotalMinor)
	}
}

func TestFakeFailureMarkers(t *testing.T) {
	f := NewFake(0, 0)
	_, err := f.Extract(context.Background(), Document{Data: []byte("xx LEDGERLINE_FAIL xx")})
	if !errors.Is(err, ErrFakeUnavailable) || IsPermanent(err) {
		t.Fatalf("want transient failure, got %v", err)
	}
	// The same outage document fails permanently afterwards, so it cannot keep
	// winning half-open probes and hold the breaker open.
	_, err = f.Extract(context.Background(), Document{Data: []byte("xx LEDGERLINE_FAIL xx")})
	if !IsPermanent(err) {
		t.Fatalf("second attempt on an outage document: want permanent, got %v", err)
	}
	if _, err = f.Extract(context.Background(), Document{Data: []byte("other LEDGERLINE_FAIL")}); !errors.Is(err, ErrFakeUnavailable) {
		t.Fatalf("a different outage document must fail transiently first, got %v", err)
	}
	always := NewFake(0, 1)
	for i := 0; i < 3; i++ {
		if _, err := always.Extract(context.Background(), Document{Data: []byte("normal")}); !errors.Is(err, ErrFakeUnavailable) {
			t.Fatalf("FailureRate=1 must always fail transiently, got %v", err)
		}
	}
	_, err = f.Extract(context.Background(), Document{Data: []byte("LEDGERLINE_REJECT")})
	if !IsPermanent(err) {
		t.Fatalf("want permanent failure, got %v", err)
	}
}

// fakeMessagesAPI stands in for the Anthropic Messages endpoint.
func fakeMessagesAPI(t *testing.T, status int, body string, check func(req map[string]any)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if check != nil {
			var req map[string]any
			if err := json.Unmarshal(raw, &req); err != nil {
				t.Errorf("request is not JSON: %v", err)
			}
			check(req)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
}

func message(stop, text string) string {
	b, _ := json.Marshal(map[string]any{
		"id": "msg_1", "type": "message", "role": "assistant", "model": "claude-opus-5-5",
		"stop_reason": stop, "content": []any{map[string]any{"type": "text", "text": text}},
		"usage": map[string]any{"input_tokens": 10, "output_tokens": 10},
	})
	return string(b)
}

func TestAnthropicParsesStructuredOutput(t *testing.T) {
	out := `{"is_receipt":true,"merchant":"Café Olimpico","merchant_tax_id":"","date":"2026-03-01","currency":"CAD",
	"subtotal_minor":1000,"taxes":[{"name":"GST","amount_minor":50},{"name":"QST","amount_minor":100}],"total_minor":1150,
	"payment_method":"card","category":"meals","line_items":[],"confidence":0.9}`
	srv := fakeMessagesAPI(t, 200, message("end_turn", out), func(req map[string]any) {
		if req["model"] != DefaultModel {
			t.Errorf("model = %v", req["model"])
		}
		oc, _ := req["output_config"].(map[string]any)
		if f, _ := oc["format"].(map[string]any); f["type"] != "json_schema" {
			t.Errorf("structured output not requested: %v", oc)
		}
		msgs, _ := req["messages"].([]any)
		if !strings.Contains(mustJSON(msgs), `"type":"image"`) {
			t.Error("image block missing")
		}
	})
	defer srv.Close()

	a := NewAnthropic(AnthropicConfig{APIKey: "test", BaseURL: srv.URL})
	r, err := a.Extract(context.Background(), Document{MediaType: "image/png", Data: []byte{0x89, 'P', 'N', 'G'}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Merchant != "Café Olimpico" || r.TotalMinor != 1150 || len(r.Taxes) != 2 || r.Provider != "anthropic" {
		t.Fatalf("unexpected receipt: %+v", r)
	}
}

func TestAnthropicErrorClassification(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		permanent bool
	}{
		{"overloaded is transient", 529, `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`, false},
		{"server error is transient", 500, `{"type":"error","error":{"type":"api_error","message":"x"}}`, false},
		{"bad request is permanent", 400, `{"type":"error","error":{"type":"invalid_request_error","message":"bad image"}}`, true},
		{"refusal is permanent", 200, message("refusal", ""), true},
		{"not a receipt is permanent", 200, message("end_turn", `{"is_receipt":false}`), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := fakeMessagesAPI(t, tc.status, tc.body, nil)
			defer srv.Close()
			a := NewAnthropic(AnthropicConfig{APIKey: "test", BaseURL: srv.URL})
			_, err := a.Extract(context.Background(), Document{MediaType: "application/pdf", Data: []byte("%PDF")})
			if err == nil {
				t.Fatal("want error")
			}
			if IsPermanent(err) != tc.permanent {
				t.Fatalf("permanent = %v, want %v (err: %v)", IsPermanent(err), tc.permanent, err)
			}
		})
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
