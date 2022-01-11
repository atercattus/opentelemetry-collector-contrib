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
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/model/pdata"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
)

type spanProcessor struct {
	config     Config
	counterVec *prometheus.CounterVec
	counter    prometheus.Counter
	logger     *zap.Logger

	logEvery        int64
	logEveryCounter int64
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

	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)

		dtracingTenant := ""
		dtracingService := ""
		rs.Resource().Attributes().Range(func(k string, v pdata.AttributeValue) bool {
			switch attribute.Key(k) {
			case semconv.ServiceNamespaceKey:
				dtracingTenant = strings.TrimSpace(v.AsString())
			case semconv.ServiceNameKey:
				dtracingService = strings.TrimSpace(v.AsString())
			}
			return true
		})

		if (dtracingTenant != "") && (dtracingService != "") {
			continue
		}

		cnt := float64(rs.InstrumentationLibrarySpans().Len())
		sp.counterVec.WithLabelValues(dtracingTenant, dtracingService).Add(cnt)
		sp.counter.Add(cnt)

		if sp.logEvery > 0 {
			sp.logEveryCounter++
			if sp.logEveryCounter == sp.logEvery {
				sp.logEveryCounter = 0

				sp.logger.Info(fmt.Sprintf(
					"will be removed %d spans (tenant:%q service:%q)",
					int(cnt),
					dtracingTenant,
					dtracingService,
				))
			}
		}

		rss.RemoveIf(func(pdata.ResourceSpans) bool {
			return true
		})
	}

	return td, nil
}
