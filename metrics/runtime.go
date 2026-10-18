package metrics

import (
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// processStart approximates the process start time where /proc is missing
// (macOS dev builds): package initialisation runs before main.
var processStart = time.Now()

// RegisterRuntimeCollectors registers the standard Go runtime (go_*) and
// process (process_*) collectors on reg, labelled node=<nodeID> like every
// veltrixdb_* series. The server uses a private registry, so without this
// call none of these series exist — and the restart alert, which reads
// process_start_time_seconds, can never fire.
//
// client_golang's process collector reads /proc and emits nothing on
// macOS. There, process_start_time_seconds alone is served from a gauge
// (the time this package was initialised), so the restart alert and its
// tests behave the same on a dev machine.
func RegisterRuntimeCollectors(reg prometheus.Registerer, nodeID string) {
	r := prometheus.WrapRegistererWith(prometheus.Labels{"node": nodeID}, reg)
	r.MustRegister(collectors.NewGoCollector())
	if _, err := os.Stat("/proc/self/stat"); err == nil {
		r.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
		return
	}
	start := float64(processStart.UnixNano()) / 1e9
	r.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "process_start_time_seconds",
		Help: "Start time of the process since unix epoch in seconds (approximated at package init: this platform has no /proc).",
	}, func() float64 { return start }))
}
