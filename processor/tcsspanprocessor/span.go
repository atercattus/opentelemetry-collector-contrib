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
	"os"
	"regexp"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

type spanProcessor struct {
	config     Config
	counterVec *prometheus.CounterVec
	counter    prometheus.Counter
	logger     *zap.Logger
	logEvery   int64
}

// newSpanProcessor returns the span processor.
func newSpanProcessor(logger *zap.Logger, counterVec *prometheus.CounterVec,
	counter prometheus.Counter, config Config) *spanProcessor {
	logEveryEnv := os.Getenv("LOG_EVERY")
	logEvery, err := strconv.ParseInt(logEveryEnv, 10, 64)
	if err != nil {
		logger.Warn("LOG_EVERY is invalid")
	}
	return &spanProcessor{
		logger:     logger,
		config:     config,
		counterVec: counterVec,
		counter:    counter,
		logEvery:   logEvery,
	}
}

func (sp *spanProcessor) processTraces(_ context.Context, td pdata.Traces) (pdata.Traces, error) {
	rss := td.ResourceSpans()
	var logCounter int64

	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.InstrumentationLibrarySpans()

		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			var idsToRemove []string

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
					logThisSpan := false
					if logCounter >= sp.logEvery && sp.logEvery > 0 {
						logThisSpan = true
						logCounter = 0
					} else if sp.logEvery > 0 {
						logCounter++
					}

					temp := fmt.Sprintf(
						"span %s from trace %s: ", s.SpanID().HexString(), s.TraceID().HexString())

					if logThisSpan {
						sp.logger.Info(temp + "will be removed")
					}

					with := prometheus.Labels{
						"tenant":  tenant.StringVal(),
						"service": service.StringVal(),
					}
					sp.counterVec.With(with).Inc()
					sp.counter.Inc()

					idsToRemove = append(idsToRemove, s.SpanID().HexString())
					continue
				}
			}

			spans.RemoveIf(func(spn pdata.Span) bool {
				for _, id := range idsToRemove {
					if id == spn.SpanID().HexString() {
						return true
					}
				}

				return false
			})
		}
	}

	return td, nil
}

func notBlank(value string) bool {
	return notBlankRe.MatchString(value)
}

var notBlankRe = regexp.MustCompile(`[^\s]+`)
