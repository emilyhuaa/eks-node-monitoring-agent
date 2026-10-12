package probe

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
	"github.com/aws/eks-node-monitoring-agent/api/monitor/resource"
	"github.com/aws/eks-node-monitoring-agent/api/probe"
)

// The reasons the test spec's checks report under.
const (
	livenessReason  = "IPAMDNotRunning"
	readinessReason = "IPAMDNotReady"
)

// testInterval is the spec interval. The test clock advances one interval
// per round.
const testInterval = 30 * time.Second

// note is one notification as the manager sees it, and the round it was sent
// in.
type note struct {
	round    int
	reason   string
	severity monitor.Severity
	resolved bool
}

// fakeManager records each notification, tagged with the round in progress.
type fakeManager struct {
	round    int
	notes    []note
	messages []string
}

func (m *fakeManager) Subscribe(resource.Type, []resource.Part) (<-chan string, error) {
	return nil, nil
}

func (m *fakeManager) Notify(_ context.Context, c monitor.Condition) error {
	m.notes = append(m.notes, note{round: m.round, reason: c.Reason, severity: c.Severity, resolved: c.Resolved})
	m.messages = append(m.messages, c.Message)
	return nil
}

// scriptedCheck answers a check from a script with one letter per run: U is
// unhealthy, V is unhealthy with a different detail and change key, W is
// unhealthy with a different detail but U's change key, E is unhealthy with no
// change key, H is healthy, and ? is a check that could not run.
type scriptedCheck struct {
	t      *testing.T
	name   string
	script string
	ran    int
}

// scriptedTransport answers the liveness (/healthz) and readiness (/readyz)
// checks from separate scripts.
type scriptedTransport map[string]*scriptedCheck

func (s scriptedTransport) Do(_ context.Context, check probe.Check) Result {
	c := s[check.Path]
	if c.ran == len(c.script) {
		c.t.Fatalf("%s check ran more than the %d times its script covers", c.name, len(c.script))
	}
	c.ran++
	switch c.script[c.ran-1] {
	case 'U':
		return Result{Outcome: OutcomeUnhealthy, Detail: "503 Service Unavailable", ChangeKey: "reason=A"}
	case 'V':
		return Result{Outcome: OutcomeUnhealthy, Detail: "500 Internal Server Error", ChangeKey: "reason=B"}
	case 'W':
		return Result{Outcome: OutcomeUnhealthy, Detail: "503 Service Unavailable: replayed", ChangeKey: "reason=A"}
	case 'E':
		return Result{Outcome: OutcomeUnhealthy, Detail: "connection refused"}
	case 'H':
		return Result{Outcome: OutcomeHealthy}
	case '?':
		return Result{Outcome: OutcomeUnknown, Detail: "check canceled"}
	}
	c.t.Fatalf("%s script %q: use only U, V, W, E, H and ?", c.name, c.script)
	return Result{}
}

// testSpec returns a spec whose liveness check reports livenessReason and,
// if withReadiness is set, whose readiness check reports readinessReason.
// Both reasons default to Fatal. The caller sets the thresholds.
func testSpec(withReadiness bool) probe.Spec {
	spec := probe.Spec{
		Subsystem: "ipamd",
		Checks: probe.Checks{
			Liveness: probe.Check{Transport: probe.TransportHTTPLoopback, Address: "127.0.0.1:8173", Path: "/healthz", ReasonOnFail: livenessReason},
		},
		Interval: metav1.Duration{Duration: testInterval},
	}
	if withReadiness {
		spec.Checks.Readiness = &probe.Check{Transport: probe.TransportHTTPLoopback, Address: "127.0.0.1:8173", Path: "/readyz", ReasonOnFail: readinessReason}
	}
	return spec
}

// reNotifyOnChange makes a check re-send its failure when its reason field
// changes.
func reNotifyOnChange(check *probe.Check) {
	check.ReportFields = []string{"reason"}
	check.ReNotifyOnChange = []string{"reason"}
}

// runScript runs one round per letter of the liveness script and returns the
// manager with everything the runner sent. Readiness runs only in rounds where
// liveness is healthy, so its script covers just those rounds. A check that
// runs more or fewer times than its script fails the test.
func runScript(t *testing.T, spec probe.Spec, liveness, readiness string) *fakeManager {
	t.Helper()
	mgr := &fakeManager{}
	r, err := NewRunner(spec, mgr, logr.Discard())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	checks := scriptedTransport{
		"/healthz": {t: t, name: livenessCheck, script: liveness},
		"/readyz":  {t: t, name: readinessCheck, script: readiness},
	}
	r.transports[probe.TransportHTTPLoopback] = checks
	r.startedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := r.startedAt
	r.now = func() time.Time {
		clock = clock.Add(spec.Interval.Duration)
		return clock
	}
	for round := 1; round <= len(liveness); round++ {
		mgr.round = round
		if err := r.runOnce(context.Background()); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
	for _, c := range checks {
		if c.ran != len(c.script) {
			t.Errorf("%s check ran %d times, but its script covers %d", c.name, c.ran, len(c.script))
		}
	}
	return mgr
}

func TestRunner(t *testing.T) {
	fatal, warning, info := monitor.SeverityFatal, monitor.SeverityWarning, monitor.SeverityInfo
	tests := []struct {
		name      string
		threshold int           // FailureThreshold
		recovery  int           // RecoveryThreshold
		grace     time.Duration // StartupGracePeriod; rounds are 30s apart
		reNotify  time.Duration // ReNotifyInterval
		onChange  bool          // set ReNotifyOnChange on every check
		// The checks' FailureSeverity; empty uses the reason's default, Fatal.
		livenessSeverity, readinessSeverity monitor.Severity
		// Each check's script (see scriptedCheck). There is one round per
		// liveness letter. An empty readiness script means no readiness check.
		liveness, readiness string
		want                []note
	}{
		{
			name:      "fires at the threshold, once per episode",
			threshold: 3,
			liveness:  "UUUUUU",
			want:      []note{{round: 3, reason: livenessReason, severity: fatal}},
		},
		{
			name:      "a healthy round resets the count, and the short streak gets a summary",
			threshold: 3,
			liveness:  "UUHUU",
			want:      []note{{round: 3, reason: livenessReason, severity: warning}},
		},
		{
			name:      "resolves when the check recovers",
			threshold: 2,
			liveness:  "UUHH",
			want: []note{
				{round: 2, reason: livenessReason, severity: fatal},
				{round: 3, reason: livenessReason, severity: fatal, resolved: true},
			},
		},
		{
			name:      "resolves after RecoveryThreshold healthy rounds in a row",
			threshold: 3,
			recovery:  2,
			liveness:  "UUUHUHH", // the failure in round 5 restarts the recovery count
			want: []note{
				{round: 3, reason: livenessReason, severity: fatal},
				{round: 7, reason: livenessReason, severity: fatal, resolved: true},
			},
		},
		{
			name:      "an Unknown round leaves the failure count alone",
			threshold: 3,
			liveness:  "UU?U",
			want:      []note{{round: 4, reason: livenessReason, severity: fatal}},
		},
		{
			name:      "an Unknown round leaves the recovery count alone",
			threshold: 1,
			recovery:  2,
			liveness:  "UH?H",
			want: []note{
				{round: 1, reason: livenessReason, severity: fatal},
				{round: 4, reason: livenessReason, severity: fatal, resolved: true},
			},
		},
		{
			name:      "startup grace holds a failure back until it ends",
			threshold: 1,
			grace:     75 * time.Second, // covers rounds 1 and 2
			liveness:  "UUU",
			want:      []note{{round: 3, reason: livenessReason, severity: fatal}},
		},
		{
			name:      "a streak that began in grace gets no summary",
			threshold: 3,
			grace:     75 * time.Second, // covers rounds 1 and 2
			liveness:  "UUHUH",
			want:      []note{{round: 5, reason: livenessReason, severity: warning}},
		},
		{
			name:              "readiness reports under its own reason and severity",
			threshold:         1,
			readinessSeverity: warning,
			liveness:          "H",
			readiness:         "U",
			want:              []note{{round: 1, reason: readinessReason, severity: warning}},
		},
		{
			// Three failed rounds in a row, but no check failed three times
			// in a row. Liveness's one-failure streak gets its summary.
			name:      "each check counts its own failures",
			threshold: 3,
			liveness:  "UHU",
			readiness: "U",
			want:      []note{{round: 2, reason: livenessReason, severity: warning}},
		},
		{
			// Readiness fires in round 1 and is not asked in round 2, when
			// liveness fires. Both recover in round 3.
			name:      "readiness is not asked while liveness fails",
			threshold: 1,
			liveness:  "HUH",
			readiness: "UH",
			want: []note{
				{round: 1, reason: readinessReason, severity: fatal},
				{round: 2, reason: livenessReason, severity: fatal},
				{round: 3, reason: livenessReason, severity: fatal, resolved: true},
				{round: 3, reason: readinessReason, severity: fatal, resolved: true},
			},
		},
		{
			name:             "a Warning check notifies once per episode and sends no resolve",
			threshold:        1,
			livenessSeverity: warning,
			liveness:         "UUHU",
			want: []note{
				{round: 1, reason: livenessReason, severity: warning},
				{round: 4, reason: livenessReason, severity: warning},
			},
		},
		{
			name:             "a Warning check re-notifies every ReNotifyInterval while failing",
			threshold:        1,
			reNotify:         time.Minute,
			livenessSeverity: warning,
			liveness:         "UUUU",
			want: []note{
				{round: 1, reason: livenessReason, severity: warning},
				{round: 3, reason: livenessReason, severity: warning},
			},
		},
		{
			name:             "a new episode restarts the re-notify interval",
			threshold:        1,
			reNotify:         time.Minute,
			livenessSeverity: warning,
			liveness:         "UUHUU",
			want: []note{
				{round: 1, reason: livenessReason, severity: warning},
				{round: 4, reason: livenessReason, severity: warning},
			},
		},
		{
			// The Warning readiness check lets the spec set an interval.
			name:              "a Fatal check is not re-notified",
			threshold:         1,
			reNotify:          time.Minute,
			readinessSeverity: warning,
			liveness:          "HUUUU",
			readiness:         "H",
			want:              []note{{round: 2, reason: livenessReason, severity: fatal}},
		},
		{
			name:             "a changed detail re-notifies while failing",
			threshold:        1,
			onChange:         true,
			livenessSeverity: warning,
			liveness:         "UUVVU",
			want: []note{
				{round: 1, reason: livenessReason, severity: warning},
				{round: 3, reason: livenessReason, severity: warning},
				{round: 5, reason: livenessReason, severity: warning},
			},
		},
		{
			name:             "a new detail with the same change key is not re-sent",
			threshold:        1,
			onChange:         true,
			livenessSeverity: warning,
			liveness:         "UWU",
			want:             []note{{round: 1, reason: livenessReason, severity: warning}},
		},
		{
			name:             "a result without a change key is not a change",
			threshold:        1,
			onChange:         true,
			livenessSeverity: warning,
			liveness:         "UEU",
			want:             []note{{round: 1, reason: livenessReason, severity: warning}},
		},
		{
			// The interval re-send in round 3 has no change key, so round 4
			// still compares against round 1.
			name:             "an interval re-send without a change key keeps the previous key",
			threshold:        1,
			reNotify:         time.Minute,
			onChange:         true,
			livenessSeverity: warning,
			liveness:         "UEEU",
			want: []note{
				{round: 1, reason: livenessReason, severity: warning},
				{round: 3, reason: livenessReason, severity: warning},
			},
		},
		{
			// Round 3 starts a new episode without a change key, so the
			// key from round 1 no longer applies.
			name:             "a new episode drops the previous change key",
			threshold:        1,
			onChange:         true,
			livenessSeverity: warning,
			liveness:         "UHEU",
			want: []note{
				{round: 1, reason: livenessReason, severity: warning},
				{round: 3, reason: livenessReason, severity: warning},
				{round: 4, reason: livenessReason, severity: warning},
			},
		},
		{
			name:             "an Unknown round is not a change",
			threshold:        1,
			onChange:         true,
			livenessSeverity: warning,
			liveness:         "U?U",
			want:             []note{{round: 1, reason: livenessReason, severity: warning}},
		},
		{
			name:             "an Unknown round does not restart the re-notify interval",
			threshold:        1,
			reNotify:         time.Minute,
			livenessSeverity: warning,
			liveness:         "U?U",
			want: []note{
				{round: 1, reason: livenessReason, severity: warning},
				{round: 3, reason: livenessReason, severity: warning},
			},
		},
		{
			// Readiness fires in round 1 and is not asked while liveness
			// fails. When it is asked again in round 4, the interval since
			// round 1 has passed.
			name:              "readiness held through a liveness failure re-sends once the interval passes",
			threshold:         1,
			reNotify:          time.Minute,
			livenessSeverity:  warning,
			readinessSeverity: warning,
			liveness:          "HUUHH",
			readiness:         "UUU",
			want: []note{
				{round: 1, reason: readinessReason, severity: warning},
				{round: 2, reason: livenessReason, severity: warning},
				{round: 4, reason: readinessReason, severity: warning},
			},
		},
		{
			name:              "readiness held through a liveness failure re-sends a changed failure",
			threshold:         1,
			onChange:          true,
			livenessSeverity:  warning,
			readinessSeverity: warning,
			liveness:          "HUH",
			readiness:         "UV",
			want: []note{
				{round: 1, reason: readinessReason, severity: warning},
				{round: 2, reason: livenessReason, severity: warning},
				{round: 3, reason: readinessReason, severity: warning},
			},
		},
		{
			name:             "a changed detail is not re-sent without ReNotifyOnChange",
			threshold:        1,
			livenessSeverity: warning,
			liveness:         "UVU",
			want:             []note{{round: 1, reason: livenessReason, severity: warning}},
		},
		{
			name:             "a change re-notify restarts the re-notify interval",
			threshold:        1,
			reNotify:         time.Minute,
			onChange:         true,
			livenessSeverity: warning,
			liveness:         "UVVVV",
			want: []note{
				{round: 1, reason: livenessReason, severity: warning},
				{round: 2, reason: livenessReason, severity: warning},
				{round: 4, reason: livenessReason, severity: warning},
			},
		},
		{
			name:             "a changed detail below the threshold waits for the first report",
			threshold:        3,
			onChange:         true,
			livenessSeverity: warning,
			liveness:         "UVUV",
			want: []note{
				{round: 3, reason: livenessReason, severity: warning},
				{round: 4, reason: livenessReason, severity: warning},
			},
		},
		{
			name:             "an Info check's summary is Info",
			threshold:        3,
			livenessSeverity: info,
			liveness:         "UH",
			want:             []note{{round: 2, reason: livenessReason, severity: info}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := testSpec(tc.readiness != "")
			spec.FailureThreshold = tc.threshold
			spec.RecoveryThreshold = tc.recovery
			spec.StartupGracePeriod = metav1.Duration{Duration: tc.grace}
			spec.ReNotifyInterval = metav1.Duration{Duration: tc.reNotify}
			spec.Checks.Liveness.FailureSeverity = tc.livenessSeverity
			if spec.Checks.Readiness != nil {
				spec.Checks.Readiness.FailureSeverity = tc.readinessSeverity
			}
			if tc.onChange {
				reNotifyOnChange(&spec.Checks.Liveness)
				if spec.Checks.Readiness != nil {
					reNotifyOnChange(spec.Checks.Readiness)
				}
			}
			if got := runScript(t, spec, tc.liveness, tc.readiness).notes; !slices.Equal(got, tc.want) {
				t.Errorf("notifications:\n got %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestRunner_Messages(t *testing.T) {
	spec := testSpec(true)
	spec.FailureThreshold = 3
	// Liveness fails in rounds 1 and 3 and recovers in round 4. The Unknown
	// round 2 is not a failure but still takes time, so the summary reports
	// 90s since the first failure. Readiness then fails in rounds 4 to 6,
	// fires, and recovers in round 7.
	got := runScript(t, spec, "U?UHHHH", "UUUH").messages
	want := []string{
		"ipamd liveness check failed 2 times and recovered 1m30s after the first failure",
		"ipamd readiness check: 503 Service Unavailable",
		"The ipamd readiness check is healthy again",
	}
	if !slices.Equal(got, want) {
		t.Errorf("messages:\n got %q\nwant %q", got, want)
	}
}

// The agent's body changes its updatedAt on every request, its detail on the
// third, and its reason on the fourth. Only the reason change is a new
// failure.
func TestRunner_ReNotifyOnChangeComparesOnlyChangeFields(t *testing.T) {
	var requests atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		reason, detail := "ValidationFailed", "bad plugin"
		if n >= 3 {
			detail = "bad plugin, replayed"
		}
		if n >= 4 {
			reason = "ReloadFailed"
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"reason":%q,"detail":%q,"updatedAt":"2026-10-23T00:00:%02dZ"}`, reason, detail, n)
	}))
	defer ts.Close()

	spec := testSpec(false)
	spec.FailureThreshold = 1
	spec.Checks.Liveness = probe.Check{
		Transport:        probe.TransportHTTPLoopback,
		Address:          strings.TrimPrefix(ts.URL, "http://"),
		Path:             "/healthz",
		ReasonOnFail:     livenessReason,
		FailureSeverity:  monitor.SeverityWarning,
		ReportFields:     []string{"reason", "detail"},
		ReNotifyOnChange: []string{"reason"},
	}
	mgr := &fakeManager{}
	r, err := NewRunner(spec, mgr, logr.Discard())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	for round := 1; round <= 5; round++ {
		mgr.round = round
		if err := r.runOnce(context.Background()); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
	wantNotes := []note{
		{round: 1, reason: livenessReason, severity: monitor.SeverityWarning},
		{round: 4, reason: livenessReason, severity: monitor.SeverityWarning},
	}
	if !slices.Equal(mgr.notes, wantNotes) {
		t.Errorf("notifications:\n got %+v\nwant %+v", mgr.notes, wantNotes)
	}
	prefix := "ipamd liveness check: GET " + ts.URL + "/healthz: 503 Service Unavailable: "
	wantMessages := []string{prefix + "reason=ValidationFailed, detail=bad plugin", prefix + "reason=ReloadFailed, detail=bad plugin, replayed"}
	if !slices.Equal(mgr.messages, wantMessages) {
		t.Errorf("messages:\n got %q\nwant %q", mgr.messages, wantMessages)
	}
}

func TestRunner_StartExitsOnContextCancel(t *testing.T) {
	spec := testSpec(false)
	spec.FailureThreshold = 1
	r, err := NewRunner(spec, &fakeManager{}, logr.Discard())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not exit on context cancellation")
	}
}
