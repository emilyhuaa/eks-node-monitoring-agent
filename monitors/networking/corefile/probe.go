// Package corefile probes eks-networking-config-agent, which applies the
// customer's CoreDNS Corefile on Auto Mode nodes.
//
// Both checks are Warnings, so neither sets a condition or triggers repair. A
// failed Corefile is a customer config problem, DNS keeps serving the previous
// Corefile or the AMI baseline, and a replaced node would apply the same
// Corefile and fail again.
package corefile

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/aws/eks-node-monitoring-agent/api/monitor"
	"github.com/aws/eks-node-monitoring-agent/api/probe"
	"github.com/aws/eks-node-monitoring-agent/pkg/reasons"
)

// Spec returns the probe. /readyz answers 503 with a JSON status for every
// outcome other than an applied or absent Corefile.
func Spec() probe.Spec {
	return probe.Spec{
		Subsystem: "eks-networking-config-agent",
		Checks: probe.Checks{
			Liveness: probe.Check{
				Transport:       probe.TransportSystemdDBus,
				Address:         AgentUnit,
				ReasonOnFail:    reasons.NetworkingConfigAgentNotRunning.Template(),
				FailureSeverity: monitor.SeverityWarning,
				// A restart passes through these while the agent stops and
				// then starts up until it is ready, which can take up to a
				// minute.
				// A crash shows as auto-restart and still counts.
				UnknownWhen: map[string][]string{"SubState": {"stop-sigterm", "start"}},
			},
			Readiness: &probe.Check{
				Transport:       probe.TransportHTTPLoopback,
				Address:         "127.0.0.1:62350",
				Path:            "/readyz",
				ReasonOnFail:    reasons.CoreDNSCorefileNotApplied.Template(),
				FailureSeverity: monitor.SeverityWarning,
				// updatedAt changes on every publish, so it stays out of the
				// detail. The free-text detail can differ when the agent
				// replays the same failure, so it does not mark a new one.
				ReportFields:     []string{"reason", "detail", "appliedSha", "failedSha"},
				ReNotifyOnChange: []string{"reason", "appliedSha", "failedSha"},
				// The agent reports Initializing before its first reconcile and
				// for node-side boot failures, neither of which is the
				// Corefile's fault.
				UnknownWhen: map[string][]string{"reason": {"CoreDNSCorefileInitializing"}},
			},
		},
		Interval: metav1.Duration{Duration: 30 * time.Second},
		// A short failure below a higher threshold would still post a
		// summary, but one without the agent's status.
		FailureThreshold: 1,
		// Covers boot until systemd starts the agent.
		StartupGracePeriod: metav1.Duration{Duration: 2 * time.Minute},
		// A rejected Corefile stays rejected until the ConfigMap changes, and
		// a stopped agent stays stopped until something restarts it, so
		// re-send before the Warning event ages out.
		ReNotifyInterval: metav1.Duration{Duration: 30 * time.Minute},
	}
}
