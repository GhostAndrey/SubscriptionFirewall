package obs

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"subscriptionfirewall/internal/version"
)

type Metrics struct {
	transactionsIngested  prometheus.Counter
	detectionRuns         prometheus.Counter
	subscriptionsDetected *prometheus.CounterVec
	subscriptionsSwept    *prometheus.CounterVec
	tokenAuthorizations   *prometheus.CounterVec
	tokensFrozen          prometheus.Counter
	enqueueDropped        prometheus.Counter
	httpRequests          *prometheus.CounterVec
	httpDuration          *prometheus.HistogramVec
	queueDepth            prometheus.Gauge
	lifecycleSync         *prometheus.CounterVec
	linkedActionFailures  *prometheus.CounterVec
	lifecycleQueueDepth   prometheus.Gauge
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
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "http_requests_total",
			Help:      "HTTP requests by method, route and status code.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "subscription_firewall",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request latency by method and route.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "subscription_firewall",
			Name:      "detection_queue_depth",
			Help:      "Current number of users waiting for detection.",
		}),
		subscriptionsSwept: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "subscriptions_swept_total",
			Help:      "Subscriptions transitioned by the overdue sweep, by resulting state.",
		}, []string{"state"}),
		lifecycleSync: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "lifecycle_sync_total",
			Help:      "Lifecycle actions applied to an entity and its counterpart, by result.",
		}, []string{"entity", "action", "result"}),
		linkedActionFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "subscription_firewall",
			Name:      "linked_action_failures_total",
			Help:      "Failures to apply an action to the entity linked to the changed one.",
		}, []string{"entity", "action"}),
		lifecycleQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "subscription_firewall",
			Name:      "lifecycle_queue_depth",
			Help:      "Pending lifecycle synchronizations awaiting the counterpart entity.",
		}),
	}
	if registerer != nil {
		registerer.MustRegister(
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Namespace: "subscription_firewall",
				Name:      "build_info",
				Help:      "Build metadata; value is always 1.",
				ConstLabels: prometheus.Labels{
					"version":    version.Version,
					"commit":     version.Commit,
					"build_date": version.BuildDate,
				},
			}, func() float64 { return 1 }),
		)
		registerer.MustRegister(
			metrics.transactionsIngested,
			metrics.detectionRuns,
			metrics.subscriptionsDetected,
			metrics.subscriptionsSwept,
			metrics.tokenAuthorizations,
			metrics.tokensFrozen,
			metrics.enqueueDropped,
			metrics.httpRequests,
			metrics.httpDuration,
			metrics.queueDepth,
			metrics.lifecycleSync,
			metrics.linkedActionFailures,
			metrics.lifecycleQueueDepth,
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
func (m *Metrics) SetQueueDepth(depth int) {
	m.queueDepth.Set(float64(depth))
}
func (m *Metrics) CountSubscriptionSwept(state string) {
	m.subscriptionsSwept.WithLabelValues(state).Inc()
}
func (m *Metrics) CountLifecycleSync(entity, action, result string) {
	m.lifecycleSync.WithLabelValues(entity, action, result).Inc()
}
func (m *Metrics) CountLinkedActionFailure(entity, action string) {
	m.linkedActionFailures.WithLabelValues(entity, action).Inc()
}
func (m *Metrics) SetLifecycleQueueDepth(depth int) {
	m.lifecycleQueueDepth.Set(float64(depth))
}
func (m *Metrics) ObserveHTTPRequest(method, route string, status int, duration time.Duration) {
	statusCode := strconv.Itoa(status)
	m.httpRequests.WithLabelValues(method, route, statusCode).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(duration.Seconds())
}
