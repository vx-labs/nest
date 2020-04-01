package stats

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func MilisecondsElapsed(from time.Time) float64 {
	return float64(time.Since(from)) / float64(time.Millisecond)
}

var (
	prometheusMetricsFactory promauto.Factory                = promauto.With(prometheus.DefaultRegisterer)
	gaugeVecs                map[string]*prometheus.GaugeVec = map[string]*prometheus.GaugeVec{}
	gauges                   map[string]prometheus.Gauge     = map[string]prometheus.Gauge{}
	histograms               map[string]prometheus.Histogram = map[string]prometheus.Histogram{
		"raftLoopProcessingTime": prometheusMetricsFactory.NewHistogram(prometheus.HistogramOpts{
			Name:    "nest_raft_loop_processing_time_milliseconds",
			Help:    "The time elapsed processing Raft events.",
			Buckets: []float64{0.5, 1, 5, 50, 100},
		}),
	}
	histogramVecs map[string]*prometheus.HistogramVec = map[string]*prometheus.HistogramVec{
		"raftRPCHandling": prometheusMetricsFactory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "nest_raft_rpc_time_miliseconds",
			Help:    "The time elasped calling raft RPCs.",
			Buckets: []float64{0.1, 5, 50},
		}, []string{"message_type", "result"}),
	}
)

func HistogramVec(name string) *prometheus.HistogramVec {
	return histogramVecs[name]
}

func GaugeVec(name string) *prometheus.GaugeVec {
	return gaugeVecs[name]
}

func Histogram(name string) prometheus.Histogram {
	return histograms[name]
}
func Gauge(name string) prometheus.Gauge {
	return gauges[name]
}

func ListenAndServe(port int) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", port), mux)
}
