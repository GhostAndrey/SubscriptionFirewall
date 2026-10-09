package obs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func counterValue(t *testing.T, registry *prometheus.Registry, name string, labels ...map[string]string) float64 {
	t.Helper()

	family := gatherFamily(t, registry, name)
	for _, metric := range family.GetMetric() {
		if matchesLabels(metric, labels) {
			return metric.GetCounter().GetValue()
		}
	}
	t.Fatalf("no counter %s matched labels %v", name, labels)
	return 0
}

func gaugeValue(t *testing.T, registry *prometheus.Registry, name string, labels ...map[string]string) float64 {
	t.Helper()

	family := gatherFamily(t, registry, name)
	for _, metric := range family.GetMetric() {
		if matchesLabels(metric, labels) {
			return metric.GetGauge().GetValue()
		}
	}
	t.Fatalf("no gauge %s matched labels %v", name, labels)
	return 0
}

func histogramValue(t *testing.T, registry *prometheus.Registry, name string, labels ...map[string]string) (uint64, float64) {
	t.Helper()

	family := gatherFamily(t, registry, name)
	for _, metric := range family.GetMetric() {
		if !matchesLabels(metric, labels) {
			continue
		}
		histogram := metric.GetHistogram()
		return histogram.GetSampleCount(), histogram.GetSampleSum()
	}
	t.Fatalf("no histogram %s matched labels %v", name, labels)
	return 0, 0
}

func gatherFamily(t *testing.T, registry *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric %s is not registered", name)
	return nil
}

func matchesLabels(metric *dto.Metric, wanted []map[string]string) bool {
	if len(wanted) == 0 {
		return true
	}
	for _, expected := range wanted {
		if matchesExactly(metric, expected) {
			return true
		}
	}
	return false
}

func matchesExactly(metric *dto.Metric, expected map[string]string) bool {
	present := make(map[string]string, len(metric.GetLabel()))
	for _, pair := range metric.GetLabel() {
		present[pair.GetName()] = pair.GetValue()
	}
	for key, value := range expected {
		if present[key] != value {
			return false
		}
	}
	return true
}
