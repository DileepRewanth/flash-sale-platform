// Package autoscale computes the replica count that a Kubernetes HPA, driven by
// KEDA's Prometheus scaler, would request from the in-flight metric. The load
// generator uses it to show, from real measurements, how that metric drives
// scaling. It is a simulation of the control loop, not a running cluster.
package autoscale

import "math"

// Desired returns ceil(metricTotal / targetPerReplica), clamped to [min, max].
// This is the HPA formula for an AverageValue target, which KEDA's Prometheus
// trigger uses: the summed metric divided by the per-replica threshold.
func Desired(metricTotal, targetPerReplica float64, min, max int) int {
	if targetPerReplica <= 0 {
		return max
	}
	n := int(math.Ceil(metricTotal / targetPerReplica))
	if n < min {
		n = min
	}
	if n > max {
		n = max
	}
	return n
}
