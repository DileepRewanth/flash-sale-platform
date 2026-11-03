// Package metrics defines every metric the service exports. Alert rules in
// deploy/prometheus and the autoscaling config in deploy/k8s use these names.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/DileepRewanth/flash-sale-platform/internal/reconcile"
)

// M holds the service's metrics on a private registry.
type M struct {
	Registry            *prometheus.Registry
	Requests            *prometheus.CounterVec
	Duration            *prometheus.HistogramVec
	InFlight            prometheus.Gauge
	Shed                *prometheus.CounterVec
	Reservations        *prometheus.CounterVec
	PaymentResults      *prometheus.CounterVec
	OversoldUnits       prometheus.Gauge
	DuplicateCharges    prometheus.Gauge
	ChargedWithoutOrder prometheus.Gauge
	StuckPayments       prometheus.Gauge
	DriftUnits          prometheus.Gauge
	DriftRepaired       prometheus.Counter
	Units               *prometheus.GaugeVec
}

// New registers all metrics on a fresh registry.
func New() *M {
	reg := prometheus.NewRegistry()
	f := promauto.With(reg)
	m := &M{
		Registry: reg,
		Requests: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashsale_http_requests_total", Help: "HTTP requests by route and status code. Shed requests use code \"shed\".",
		}, []string{"route", "code"}),
		Duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Name: "flashsale_http_request_duration_seconds", Help: "Server-side request latency by route.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
		}, []string{"route"}),
		InFlight: f.NewGauge(prometheus.GaugeOpts{
			Name: "flashsale_http_inflight_requests", Help: "Requests in progress. This is the autoscaling signal.",
		}),
		Shed: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashsale_http_shed_total", Help: "Requests rejected by load shedding, by route.",
		}, []string{"route"}),
		Reservations: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashsale_reservations_total", Help: "Reservation attempts by result.",
		}, []string{"result"}),
		PaymentResults: f.NewCounterVec(prometheus.CounterOpts{
			Name: "flashsale_payment_results_total", Help: "Provider results applied by the state machine.",
		}, []string{"result"}),
		OversoldUnits: f.NewGauge(prometheus.GaugeOpts{
			Name: "flashsale_oversold_units", Help: "Units sold more than once. Must be zero.",
		}),
		DuplicateCharges: f.NewGauge(prometheus.GaugeOpts{
			Name: "flashsale_duplicate_charges", Help: "Extra kept charges on an order. Must be zero.",
		}),
		ChargedWithoutOrder: f.NewGauge(prometheus.GaugeOpts{
			Name: "flashsale_charged_without_order", Help: "Orders charged but neither confirmed nor refunding.",
		}),
		StuckPayments: f.NewGauge(prometheus.GaugeOpts{
			Name: "flashsale_stuck_payments", Help: "Payments still unresolved past the hard deadline.",
		}),
		DriftUnits: f.NewGauge(prometheus.GaugeOpts{
			Name: "flashsale_inventory_drift_units", Help: "Units where Redis disagrees with the durable store.",
		}),
		DriftRepaired: f.NewCounter(prometheus.CounterOpts{
			Name: "flashsale_inventory_drift_repaired_total", Help: "Units repaired in Redis by the drift repairer.",
		}),
		Units: f.NewGaugeVec(prometheus.GaugeOpts{
			Name: "flashsale_units", Help: "Units by durable state.",
		}, []string{"state"}),
	}
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// SetReport publishes a detector run.
func (m *M) SetReport(r reconcile.Report) {
	m.OversoldUnits.Set(float64(r.OversoldUnits))
	m.DuplicateCharges.Set(float64(r.DuplicateCharges))
	m.ChargedWithoutOrder.Set(float64(r.ChargedWithoutOrder))
	m.StuckPayments.Set(float64(r.StuckPayments))
	m.DriftUnits.Set(float64(r.DriftUnits))
	m.Units.WithLabelValues("free").Set(float64(r.FreeUnits))
	m.Units.WithLabelValues("reserved").Set(float64(r.ReservedUnits))
	m.Units.WithLabelValues("sold").Set(float64(r.SoldUnits))
}
