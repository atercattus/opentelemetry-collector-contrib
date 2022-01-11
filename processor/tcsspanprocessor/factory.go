// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tcsspanprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tcsspanprocessor"

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

const (
	// typeStr is the value of "type" Span processor in the configuration.
	typeStr = "tcsspan"
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

// NewFactory returns a new factory for the Span processor.
func NewFactory() component.ProcessorFactory {
	return processorhelper.NewFactory(
		typeStr,
		createDefaultConfig,
		processorhelper.WithTraces(createTracesProcessor))
}

func createDefaultConfig() config.Processor {
	return &Config{
		ProcessorSettings: config.NewProcessorSettings(config.NewComponentID(typeStr)),
	}
}

func createTracesProcessor(
	_ context.Context,
	set component.ProcessorCreateSettings,
	cfg config.Processor,
	nextConsumer consumer.Traces,
) (component.TracesProcessor, error) {

	oCfg := cfg.(*Config)

	counterVec := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "spans_without_attrs_vector"}, []string{"dtracing_tenant", "dtracing_service"},
	)
	counter := prometheus.NewCounter(
		prometheus.CounterOpts{Name: "spans_without_attrs_linear"},
	)

	prometheus.MustRegister(counter, counterVec)

	sp := newSpanProcessor(set.Logger, counterVec, counter, *oCfg)

	return processorhelper.NewTracesProcessor(
		cfg,
		nextConsumer,
		sp.processTraces,
		processorhelper.WithCapabilities(processorCapabilities),
	)
}
