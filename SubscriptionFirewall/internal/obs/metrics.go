package obs

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	transactionsIngested  prometheus.Counter
	detectionRuns         prometheus.Counter
	subscriptionsDetected *prometheus.CounterVec
	tokenAuthorizations   *prometheus.CounterVec
	tokensFrozen          prometheus.Counter
	enqueueDropped        prometheus.Counter
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		transactionsIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "transactions_ingested_total",
			Help:      "Total accepted transactions.",
		}),
		detectionRuns: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "detection_runs_total",
			Help:      "Total detection sweeps over user transaction history.",
		}),
		subscriptionsDetected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "subscriptions_detected_total",
			Help:      "Detected subscriptions by lifecycle state.",
		}, []string{"state"}),
		tokenAuthorizations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "token_authorizations_total",
			Help:      "Virtual token authorization attempts by result.",
		}, []string{"result"}),
		tokensFrozen: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "tokens_frozen_total",
			Help:      "Instant virtual token freezes.",
		}),
		enqueueDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "detection_enqueue_dropped_total",
			Help:      "Detection requests dropped because the queue was full.",
		}),
	}
	if registerer != nil {
		registerer.MustRegister(
			metrics.transactionsIngested,
			metrics.detectionRuns,
			metrics.subscriptionsDetected,
			metrics.tokenAuthorizations,
			metrics.tokensFrozen,
			metrics.enqueueDropped,
		)
	}
	return metrics
}

func (m *Metrics) CountTransactionIngested() { m.transactionsIngested.Inc() }
func (m *Metrics) CountDetectionRun()        { m.detectionRuns.Inc() }
func (m *Metrics) CountEnqueueDropped()      { m.enqueueDropped.Inc() }
func (m *Metrics) CountTokenFrozen()         { m.tokensFrozen.Inc() }
func (m *Metrics) CountSubscriptionDetected(state string) {
	m.subscriptionsDetected.WithLabelValues(state).Inc()
}
func (m *Metrics) CountTokenAuthorization(result string) {
	m.tokenAuthorizations.WithLabelValues(result).Inc()
}
