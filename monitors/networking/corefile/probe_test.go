package corefile

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
	"github.com/aws/eks-node-monitoring-agent/pkg/probe"
)

func TestSpecIsValid(t *testing.T) {
	if err := probe.Validate(Spec()); err != nil {
		t.Fatal(err)
	}
}

func TestSpecChecks(t *testing.T) {
	spec := Spec()
	liveness, readiness := spec.Checks.Liveness, spec.Checks.Readiness
	if liveness.Address != AgentUnit || liveness.FailureSeverity != monitor.SeverityWarning {
		t.Errorf("liveness should be a Warning on %s, got %+v", AgentUnit, liveness)
	}
	if readiness == nil {
		t.Fatal("readiness check is missing")
	}
	if readiness.Address != "127.0.0.1:62350" || readiness.Path != "/readyz" || readiness.FailureSeverity != monitor.SeverityWarning {
		t.Errorf("readiness should be a Warning on 127.0.0.1:62350/readyz, got %+v", readiness)
	}
	if want := []string{"reason", "appliedSha", "failedSha"}; !slices.Equal(readiness.ReNotifyOnChange, want) {
		t.Errorf("readiness should re-notify when %v changes, got %v", want, readiness.ReNotifyOnChange)
	}
	// A crash leaves the unit in auto-restart, which must still count.
	if states := liveness.UnknownWhen["SubState"]; !slices.Contains(states, "start") || slices.Contains(states, "auto-restart") {
		t.Errorf("liveness should ride through a restart but not a crash, got UnknownWhen %v", liveness.UnknownWhen)
	}
}

// readyzBody is a 503 body as the agent writes it.
func readyzBody(reason, detail, failedSHA, updatedAt string) string {
	return fmt.Sprintf(`{"reason":%q,"detail":%q,"appliedSha":"aaa","failedSha":%q,"updatedAt":%q,"nodeName":"node-1"}`,
		reason, detail, failedSHA, updatedAt)
}

// readiness answers the spec's readiness check from a fake agent that serves
// the given 503 body.
func readiness(t *testing.T, body string) (probe.Result, string) {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(body))
	}))
	defer ts.Close()
	check := *Spec().Checks.Readiness
	check.Address = strings.TrimPrefix(ts.URL, "http://")
	return probe.NewHTTPLoopbackTransport().Do(context.Background(), check), ts.URL
}

func TestSpecReadinessDetail(t *testing.T) {
	first, url := readiness(t, readyzBody("CoreDNSCorefileValidationFailed", "plugin foo is not allowed", "bbb", "2026-10-23T00:00:00Z"))
	want := "GET " + url + "/readyz: 503 Service Unavailable: " +
		"reason=CoreDNSCorefileValidationFailed, detail=plugin foo is not allowed, appliedSha=aaa, failedSha=bbb"
	if first.Outcome != probe.OutcomeUnhealthy || first.Detail != want {
		t.Errorf("result:\n got %s %q\nwant Unhealthy %q", first.Outcome, first.Detail, want)
	}
	replayed, _ := readiness(t, readyzBody("CoreDNSCorefileValidationFailed", "cfg unchanged since prior failure", "bbb", "2026-10-23T00:00:30Z"))
	if replayed.ChangeKey != first.ChangeKey {
		t.Errorf("a replay with a new detail and updatedAt changed the change key from %q to %q", first.ChangeKey, replayed.ChangeKey)
	}
	edited, _ := readiness(t, readyzBody("CoreDNSCorefileValidationFailed", "plugin foo is not allowed", "ccc", "2026-10-23T00:01:00Z"))
	if edited.ChangeKey == first.ChangeKey {
		t.Error("a different rejected Corefile should change the change key")
	}
}

func TestSpecReadinessInitializingIsUnknown(t *testing.T) {
	result, _ := readiness(t, readyzBody("CoreDNSCorefileInitializing", "agent starting; no reconcile yet", "", "2026-10-23T00:00:00Z"))
	if result.Outcome != probe.OutcomeUnknown {
		t.Errorf("outcome = %q (%s), want Unknown so Initializing never fires", result.Outcome, result.Detail)
	}
}
