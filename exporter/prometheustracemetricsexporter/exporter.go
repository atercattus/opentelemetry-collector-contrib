// Copyright  OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheustracemetricsexporter

import (
	"context"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
)

type (
	exporter struct {
		cfg *Config

		spanCounter   prometheus.Counter
		spanBatchSize prometheus.Gauge

		marshaler pdata.TracesMarshaler
	}
)

const (
	metricsNamespace = "promtracemetrics"
)

var (
	_ component.TracesExporter = &exporter{}
)

func newExporter(cfg *Config) *exporter {
	spanCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Name:      "span_count",
	})
	prometheus.MustRegister(spanCounter)

	spanBatchSize := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "span_batch_size",
	})
	prometheus.MustRegister(spanBatchSize)

	return &exporter{
		cfg: cfg,

		spanCounter:   spanCounter,
		spanBatchSize: spanBatchSize,

		marshaler: otlp.NewProtobufTracesMarshaler(),
	}
}

func (e *exporter) Start(ctx context.Context, host component.Host) error {
	go func() {
		err := http.ListenAndServe(e.cfg.ScrapeListenAddr, promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{
				// EnableOpenMetrics: true,
			},
		))
		if err != nil {
			log.Printf("Could not serve trace metrics properly: %s", err)
		}
	}()

	return nil
}

func (e *exporter) Shutdown(ctx context.Context) error {
	log.Printf("exporter.Shutdown")
	return nil
}

func (e *exporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *exporter) ConsumeTraces(ctx context.Context, td pdata.Traces) error {
	raw, err := e.marshaler.MarshalTraces(td)
	if err != nil {
		log.Printf("Could not marshal traces: %s", err)
		return nil
	}

	e.spanCounter.Add(float64(td.SpanCount()))
	e.spanBatchSize.Add(float64(len(raw)))

	return nil
}
