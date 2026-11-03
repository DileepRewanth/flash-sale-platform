package autoscale

import "testing"

func TestDesired(t *testing.T) {
	cases := []struct {
		metric, target float64
		min, max, want int
	}{
		{120, 50, 2, 100, 3},    // ceil(120/50)
		{10, 50, 2, 100, 2},     // floor at min
		{9000, 50, 2, 100, 100}, // capped at max
		{100, 0, 2, 100, 100},   // a broken target fails open to max
	}
	for _, c := range cases {
		if got := Desired(c.metric, c.target, c.min, c.max); got != c.want {
			t.Errorf("Desired(%v, %v, %d, %d) = %d, want %d", c.metric, c.target, c.min, c.max, got, c.want)
		}
	}
}
