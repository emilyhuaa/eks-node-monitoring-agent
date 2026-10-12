// Package probe defines the declarative contract for subagent health probes.
//
// A probe declares how the node monitoring agent observes the health of one
// AWS-owned subagent (e.g. IPAMD, the Network Policy Agent) through the
// agent's own health surface. Probes are pure data: the framework in
// pkg/probe interprets a Spec and emits conditions through the parent
// monitor, so onboarding an agent is a spec addition rather than new
// detection code.
package probe

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
)

// TransportKind identifies how a Check reaches its target agent.
type TransportKind string

const (
	// TransportHTTPLoopback performs an HTTP GET against a loopback address
	// and port, such as 127.0.0.1:8173 or localhost:8173. A 2xx response is
	// healthy; any other response, including a redirect or a refused
	// connection, is unhealthy.
	TransportHTTPLoopback TransportKind = "http-loopback"
	// TransportSystemdDBus queries the ActiveState of a systemd service unit
	// over D-Bus. Liveness only: "active" is healthy, anything else is
	// unhealthy, including a unit that does not exist. A failure to reach
	// D-Bus, including a call that times out, is unknown, not unhealthy.
	TransportSystemdDBus TransportKind = "systemd-dbus"
)

// Spec declares a health probe for one AWS-owned subagent. It is fully
// JSON/YAML-serializable so that probe registration can later move from
// binary-baked defaults to the agent's configuration surface without
// reshaping the contract.
type Spec struct {
	// Subsystem identifies the target agent, e.g. "ipamd" or
	// "network-policy-agent". Used in logs and condition messages.
	Subsystem string `json:"subsystem"`

	// Checks defines the agent's health surface.
	Checks Checks `json:"checks"`

	// Interval is the time between check rounds.
	Interval metav1.Duration `json:"interval"`

	// FailureThreshold is the number of consecutive unhealthy results a
	// check needs before its failure is emitted. Each check counts its own
	// results, and the count resets on that check's next healthy result.
	// Must be at least 1.
	FailureThreshold int `json:"failureThreshold"`

	// RecoveryThreshold is the number of consecutive healthy results a check
	// needs before its emitted failure is resolved.
	// It damps condition flapping: every False→True transition resets Auto
	// Repair's wait clock, so a flapping agent would otherwise never be
	// repaired. If zero, it defaults to 1, which resolves on the first
	// healthy result and preserves the pre-hysteresis behavior.
	RecoveryThreshold int `json:"recoveryThreshold,omitempty"`

	// StartupGracePeriod suppresses failure emission for this duration after
	// the runner starts, tolerating agents that come up after the monitoring
	// agent during boot.
	StartupGracePeriod metav1.Duration `json:"startupGracePeriod,omitempty"`

	// ReNotifyInterval re-sends a Warning or Info failure that is still
	// failing once this long has passed since it was last sent, so its event
	// does not age out while the failure persists. If zero, a failure is sent
	// once per episode. A Fatal failure keeps its condition and is not
	// re-sent.
	ReNotifyInterval metav1.Duration `json:"reNotifyInterval,omitempty"`
}

// Checks defines the individual checks that make up a probe. Liveness is
// required; readiness and diagnostics are optional because not every
// transport can express them (systemd ActiveState has no notion of
// readiness).
type Checks struct {
	// Liveness answers "is the agent running".
	Liveness Check `json:"liveness"`
	// Readiness answers "is the agent performing its function". The depth of
	// the readiness answer is owned by the agent, not by this contract. It is
	// asked only while liveness is healthy.
	Readiness *Check `json:"readiness,omitempty"`
	// Diagnostics optionally returns free-form structured detail that is
	// surfaced as informational events, never as condition flips. It has no
	// pass or fail, so it sets neither ReasonOnFail nor FailureSeverity.
	Diagnostics *Check `json:"diagnostics,omitempty"`
}

// Check is a single health question asked over a specific transport, and
// what to report when the answer is unhealthy.
type Check struct {
	// Transport selects how the target is reached.
	Transport TransportKind `json:"transport"`
	// Address is the transport-specific target, e.g. "127.0.0.1:8901" for
	// http-loopback or a service unit name such as "ipamd.service" for
	// systemd-dbus.
	Address string `json:"address"`
	// Path is the HTTP request path, e.g. "/healthz". Required for
	// http-loopback and must be empty for systemd-dbus.
	Path string `json:"path,omitempty"`

	// ReasonOnFail is the reason identifier (a key in reasons.yaml) emitted
	// when this check fails. Liveness and readiness each need their own
	// registered, non-parameterized reason, so a repair rule can tell a dead
	// agent from one that is running but not doing its job. This is
	// validated at startup.
	ReasonOnFail string `json:"reasonOnFail,omitempty"`

	// FailureSeverity is the severity of the condition emitted when this
	// check fails. If empty, the reason's default severity from reasons.yaml
	// is used.
	FailureSeverity monitor.Severity `json:"failureSeverity,omitempty"`

	// ReportFields names the top-level fields of a JSON response body to
	// report, in order, instead of the raw body, e.g. ["reason", "detail"].
	// It keeps volatile fields such as timestamps out of the failure detail.
	// A field that is missing, null, or an empty string is left out. A body
	// that is not a JSON object, or has none of the fields, is reported raw.
	// http-loopback only.
	ReportFields []string `json:"reportFields,omitempty"`

	// ReNotifyOnChange names report fields whose values identify a failure.
	// A failure that is still failing is sent again when any of them
	// changes, so a new failure is reported without waiting for the check to
	// recover. Each must also be in ReportFields, so the new event shows the
	// change. Warning and Info checks only: a Fatal check holds one condition
	// for the whole episode.
	ReNotifyOnChange []string `json:"reNotifyOnChange,omitempty"`

	// UnknownWhen makes a result Unknown instead of Unhealthy when a named
	// value has one of the listed strings. An Unknown result neither counts
	// toward a threshold nor fires. For http-loopback the names are
	// top-level fields of a non-2xx JSON response body, e.g.
	// {"reason": ["Initializing"]}. For systemd-dbus they are the unit's
	// ActiveState, LoadState, SubState, or Result when it is not active.
	UnknownWhen map[string][]string `json:"unknownWhen,omitempty"`
}
