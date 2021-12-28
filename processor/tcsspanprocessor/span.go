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
	"fmt"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

type spanProcessor struct {
	config  Config
	counter *prometheus.CounterVec
	logger  *zap.Logger
}

// newSpanProcessor returns the span processor.
func newSpanProcessor(logger *zap.Logger, counter *prometheus.CounterVec, config Config) *spanProcessor {
	return &spanProcessor{
		logger:  logger,
		config:  config,
		counter: counter,
	}
}

func (sp *spanProcessor) processTraces(_ context.Context, td pdata.Traces) (pdata.Traces, error) {
	rss := td.ResourceSpans()

	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.InstrumentationLibrarySpans()

		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()

			for k := 0; k < spans.Len(); k++ {
				s := spans.At(k)
				remove := false

				tenant, found := s.Attributes().Get("tenant")
				if !found || !notBlank(tenant.StringVal()) {
					remove = true
				}

				service, found := s.Attributes().Get("service")
				if !found || !notBlank(service.StringVal()) {
					remove = true
				}

				if remove {
					temp := fmt.Sprintf("span %s from trace %s: ", s.SpanID(), s.TraceID())
					sp.logger.Info(temp + "is going to be removed")

					with := prometheus.Labels{
						"tenant":  tenant.StringVal(),
						"service": service.StringVal(),
					}
					sp.counter.With(with).Inc()

					spans.RemoveIf(func(spn pdata.Span) bool {
						rm := s.SpanID().HexString() == spn.SpanID().HexString()
						if rm {
							sp.logger.Info(temp + "RemoveIf returns true")
						}

						return rm
					})

					continue
				}
			}
		}
	}

	return td, nil
}

func notBlank(value string) bool {
	return notBlankRe.MatchString(value)
}

var notBlankRe = regexp.MustCompile(`[^\s]+`)
