package probe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/aws/eks-node-monitoring-agent/api/probe"
)

func httpCheck(address string) probe.Check {
	return probe.Check{Transport: probe.TransportHTTPLoopback, Address: address, Path: "/healthz"}
}

func TestHTTPLoopbackTransport_Healthy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	result := NewHTTPLoopbackTransport().Do(context.Background(), httpCheck(strings.TrimPrefix(ts.URL, "http://")))
	if result.Outcome != OutcomeHealthy {
		t.Fatalf("outcome = %q (%s), want Healthy", result.Outcome, result.Detail)
	}
}

func TestHTTPLoopbackTransport_UnhealthyStatusWithBodyDetail(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"ebpf map write failed"}`))
	}))
	defer ts.Close()

	result := NewHTTPLoopbackTransport().Do(context.Background(), httpCheck(strings.TrimPrefix(ts.URL, "http://")))
	if result.Outcome != OutcomeUnhealthy {
		t.Fatalf("outcome = %q, want Unhealthy", result.Outcome)
	}
	if !strings.Contains(result.Detail, "503") || !strings.Contains(result.Detail, "ebpf map write failed") {
		t.Errorf("detail %q should contain status and body", result.Detail)
	}
}

func TestHTTPLoopbackTransport_MarksTruncatedBody(t *testing.T) {
	for _, tc := range []struct {
		name          string
		bodyLen       int
		wantTruncated bool
	}{
		{"body at the limit", maxDetailBytes, false},
		{"body past the limit", maxDetailBytes + 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(strings.Repeat("x", tc.bodyLen)))
			}))
			defer ts.Close()

			result := NewHTTPLoopbackTransport().Do(context.Background(), httpCheck(strings.TrimPrefix(ts.URL, "http://")))
			kept := strings.Repeat("x", maxDetailBytes)
			if !strings.Contains(result.Detail, kept) || strings.Contains(result.Detail, kept+"x") {
				t.Errorf("detail should keep exactly %d body bytes, got %q", maxDetailBytes, result.Detail)
			}
			if got := strings.HasSuffix(result.Detail, " (truncated)"); got != tc.wantTruncated {
				t.Errorf("truncation marker = %v, want %v: %q", got, tc.wantTruncated, result.Detail)
			}
		})
	}
}

func TestHTTPLoopbackTransport_RedirectIsUnhealthyAndNotFollowed(t *testing.T) {
	var followed atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/healthz", http.StatusFound)
	}))
	defer ts.Close()

	result := NewHTTPLoopbackTransport().Do(context.Background(), httpCheck(strings.TrimPrefix(ts.URL, "http://")))
	if result.Outcome != OutcomeUnhealthy {
		t.Fatalf("outcome = %q (%s), want Unhealthy for a redirect", result.Outcome, result.Detail)
	}
	if !strings.Contains(result.Detail, "302") {
		t.Errorf("detail %q should show the redirect status", result.Detail)
	}
	if followed.Load() {
		t.Error("the transport followed the redirect")
	}
}

func TestHTTPLoopbackTransport_ConnectionRefusedIsUnhealthy(t *testing.T) {
	// Reserve a port, then close the listener so the connection is refused.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := strings.TrimPrefix(ts.URL, "http://")
	ts.Close()

	result := NewHTTPLoopbackTransport().Do(context.Background(), httpCheck(addr))
	if result.Outcome != OutcomeUnhealthy {
		t.Fatalf("outcome = %q (%s), want Unhealthy for refused connection", result.Outcome, result.Detail)
	}
}

func TestHTTPLoopbackTransport_CanceledContextIsUnknown(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := NewHTTPLoopbackTransport().Do(ctx, httpCheck(strings.TrimPrefix(ts.URL, "http://")))
	if result.Outcome != OutcomeUnknown {
		t.Fatalf("outcome = %q, want Unknown for canceled context", result.Outcome)
	}
}

func TestHTTPLoopbackTransport_ReportFields(t *testing.T) {
	fields := []string{"reason", "detail", "attempts", "failedSha"}
	for _, tc := range []struct {
		name, body, want string
	}{
		{
			name: "reports only the named fields, in order",
			body: `{"updatedAt":"2026-10-23T00:00:00Z","detail":"plugin foo is not allowed","reason":"ValidationFailed","failedSha":"abc"}`,
			want: "reason=ValidationFailed, detail=plugin foo is not allowed, failedSha=abc",
		},
		{
			name: "leaves out missing, null, and empty fields",
			body: `{"reason":"ValidationFailed","detail":"","failedSha":null}`,
			want: "reason=ValidationFailed",
		},
		{
			name: "writes a non-string value as JSON",
			body: `{"reason":"ReloadFailed","attempts":3}`,
			want: "reason=ReloadFailed, attempts=3",
		},
		{
			name: "reports a body that is not JSON raw",
			body: "agent starting",
			want: "agent starting",
		},
		{
			name: "reports an object without any of the fields raw",
			body: `{"status":"broken"}`,
			want: `{"status":"broken"}`,
		},
		{
			name: "reports a JSON null body raw",
			body: "null",
			want: "null",
		},
		{
			name: "reports a JSON body that is not an object raw",
			body: `["reason"]`,
			want: `["reason"]`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(tc.body))
			}))
			defer ts.Close()

			check := httpCheck(strings.TrimPrefix(ts.URL, "http://"))
			check.ReportFields = fields
			result := NewHTTPLoopbackTransport().Do(context.Background(), check)
			want := "GET " + ts.URL + "/healthz: 503 Service Unavailable: " + tc.want
			if result.Detail != want {
				t.Errorf("detail:\n got %q\nwant %q", result.Detail, want)
			}
		})
	}
}

func TestHTTPLoopbackTransport_TruncatesRenderedFields(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"detail":"` + strings.Repeat("x", 2*maxDetailBytes) + `","updatedAt":"now"}`))
	}))
	defer ts.Close()

	check := httpCheck(strings.TrimPrefix(ts.URL, "http://"))
	check.ReportFields = []string{"detail"}
	result := NewHTTPLoopbackTransport().Do(context.Background(), check)
	want := "detail=" + strings.Repeat("x", maxDetailBytes-len("detail=")) + " (truncated)"
	if !strings.HasSuffix(result.Detail, ": "+want) {
		t.Errorf("detail should end with the rendered fields cut to %d bytes, got %q", maxDetailBytes, result.Detail)
	}
}

func TestHTTPLoopbackTransport_TruncatesOnARuneBoundary(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		// The odd first byte puts the limit inside a two-byte rune.
		w.Write([]byte("x" + strings.Repeat("é", maxDetailBytes)))
	}))
	defer ts.Close()

	result := NewHTTPLoopbackTransport().Do(context.Background(), httpCheck(strings.TrimPrefix(ts.URL, "http://")))
	if !utf8.ValidString(result.Detail) || !strings.HasSuffix(result.Detail, "é (truncated)") {
		t.Errorf("detail should be cut between runes and marked truncated, got %q", result.Detail)
	}
}

// serve503 starts a server that answers every request 503 with body.
func serve503(t *testing.T, body string) probe.Check {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(body))
	}))
	t.Cleanup(ts.Close)
	return httpCheck(strings.TrimPrefix(ts.URL, "http://"))
}

func TestHTTPLoopbackTransport_UnknownWhen(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       Outcome
	}{
		{"a listed value is Unknown", `{"reason":"Initializing"}`, OutcomeUnknown},
		{"another value is Unhealthy", `{"reason":"ValidationFailed"}`, OutcomeUnhealthy},
		{"a missing field is Unhealthy", `{"detail":"Initializing"}`, OutcomeUnhealthy},
		{"a body that is not JSON is Unhealthy", "Initializing", OutcomeUnhealthy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := serve503(t, tc.body)
			check.UnknownWhen = map[string][]string{"reason": {"Starting", "Initializing"}}
			if result := NewHTTPLoopbackTransport().Do(context.Background(), check); result.Outcome != tc.want {
				t.Errorf("outcome = %q (%s), want %q", result.Outcome, result.Detail, tc.want)
			}
		})
	}
}

func TestHTTPLoopbackTransport_ChangeKey(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
	}{
		{"renders only the change fields", `{"reason":"ValidationFailed","detail":"bad plugin","failedSha":"abc","updatedAt":"now"}`, "reason=ValidationFailed, failedSha=abc"},
		{"is empty for a body that is not JSON", "agent starting", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			check := serve503(t, tc.body)
			check.ReportFields = []string{"reason", "detail", "failedSha"}
			check.ReNotifyOnChange = []string{"reason", "failedSha"}
			if got := NewHTTPLoopbackTransport().Do(context.Background(), check).ChangeKey; got != tc.want {
				t.Errorf("change key = %q, want %q", got, tc.want)
			}
		})
	}
}
