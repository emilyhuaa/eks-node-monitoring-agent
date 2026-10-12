package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/eks-node-monitoring-agent/api/probe"
)

const (
	// httpCheckTimeout bounds a single HTTP check execution.
	httpCheckTimeout = 5 * time.Second
	// maxDetailBytes bounds how much of a response body is captured into the
	// result detail for condition messages. A longer body is cut off and
	// marked as truncated.
	maxDetailBytes = 1024
	// maxBodyBytes bounds how much of a response body is read. It is larger
	// than maxDetailBytes so a body can be parsed for its report fields
	// before the detail is cut off.
	maxBodyBytes = 64 << 10
)

// HTTPLoopbackTransport executes checks as HTTP GET requests against
// local-only addresses. A 2xx response is healthy. Any non-2xx response or a
// failure to connect to the agent's endpoint is unhealthy — an agent that is
// not answering its own health surface is not healthy. Redirects are not
// followed, so a check never leaves the node and a 3xx response is unhealthy.
type HTTPLoopbackTransport struct {
	client *http.Client
}

// NewHTTPLoopbackTransport returns an HTTP transport with a bounded
// per-check timeout that does not follow redirects.
func NewHTTPLoopbackTransport() *HTTPLoopbackTransport {
	return &HTTPLoopbackTransport{
		client: &http.Client{
			Timeout: httpCheckTimeout,
			// Return a redirect as the response instead of following it:
			// address validation only covers the first request.
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func (t *HTTPLoopbackTransport) Do(ctx context.Context, check probe.Check) Result {
	url := "http://" + check.Address + check.Path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		// A malformed spec, not an agent failure.
		return Result{Outcome: OutcomeUnknown, Detail: fmt.Sprintf("building request for %s: %v", url, err)}
	}
	resp, err := t.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			// The agent is shutting down or the caller gave up; do not
			// attribute this to the probed agent.
			return Result{Outcome: OutcomeUnknown, Detail: fmt.Sprintf("check canceled: %v", ctx.Err())}
		}
		return Result{Outcome: OutcomeUnhealthy, Detail: fmt.Sprintf("GET %s: %v", url, err)}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return Result{Outcome: OutcomeHealthy}
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	object := jsonObject(body)
	detail := fmt.Sprintf("GET %s: %s", url, resp.Status)
	if b := bodyDetail(body, object, check.ReportFields); b != "" {
		detail += ": " + b
	}
	outcome := OutcomeUnhealthy
	if matchesUnknownWhen(check.UnknownWhen, func(name string) string { return fieldText(object[name]) }) {
		outcome = OutcomeUnknown
	}
	return Result{Outcome: outcome, Detail: detail, ChangeKey: renderFields(object, check.ReNotifyOnChange)}
}

// jsonObject parses a body as a JSON object, or returns nil if it is not one.
func jsonObject(body []byte) map[string]json.RawMessage {
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil {
		return nil
	}
	return object
}

// bodyDetail renders a response body for a failure detail: the report fields
// when they are set and present in the body, and the raw body otherwise.
func bodyDetail(body []byte, object map[string]json.RawMessage, fields []string) string {
	text := renderFields(object, fields)
	if text == "" {
		text = strings.TrimSpace(string(body))
	}
	return truncateDetail(text)
}

// renderFields renders the named top-level fields of a JSON object as
// "name=value" pairs. A string value is written as is and any other value as
// its JSON text.
func renderFields(object map[string]json.RawMessage, fields []string) string {
	var pairs []string
	for _, name := range fields {
		if value := fieldText(object[name]); value != "" {
			pairs = append(pairs, name+"="+value)
		}
	}
	return strings.Join(pairs, ", ")
}

// fieldText renders one JSON value. A missing or null value renders empty.
func fieldText(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return string(raw)
}

// truncateDetail cuts text to maxDetailBytes on a rune boundary and marks it
// as truncated.
func truncateDetail(text string) string {
	if len(text) <= maxDetailBytes {
		return text
	}
	cut := maxDetailBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + " (truncated)"
}
