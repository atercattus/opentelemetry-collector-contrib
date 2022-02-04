// Copyright  The OpenTelemetry Authors
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

package trace // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tcsotlpreceiver/internal/trace"

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/otlpgrpc"
	"go.opentelemetry.io/collector/model/pdata"
	"go.opentelemetry.io/collector/obsreport"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.uber.org/zap"
)

const (
	dataFormatProtobuf = "protobuf"
	receiverTransport  = "grpc"
)

// Receiver is the type used to handle spans from OpenTelemetry exporters.
type Receiver struct {
	logger       *zap.Logger
	nextConsumer consumer.Traces
	obsrecv      *obsreport.Receiver
	mutex        sync.Mutex

	collectorMinDuration   time.Duration // минимальное количество времени между запросами (1 / rps)
	collectorLastRequestAt time.Time     // время последнего запроса
	collectorCounter       prometheus.Counter

	tenantMinDuration   time.Duration
	tenantLastRequestAt map[string]time.Time
	tenantCounter       *prometheus.CounterVec

	tenantServiceMinDuration   time.Duration
	tenantServiceLastRequestAt map[string]time.Time
	tenantServiceCounter       *prometheus.CounterVec
}

// New creates a new Receiver reference.
func New(
	id config.ComponentID, nextConsumer consumer.Traces, set component.ReceiverCreateSettings,
	collectorMinDuration, tenantMinDuration, tenantServiceMinDuration time.Duration,
	collectorCounter prometheus.Counter, tenantCounter, tenantServiceCounter *prometheus.CounterVec,
) *Receiver {
	return &Receiver{
		logger:       set.Logger,
		nextConsumer: nextConsumer,
		obsrecv: obsreport.NewReceiver(obsreport.ReceiverSettings{
			ReceiverID:             id,
			Transport:              receiverTransport,
			ReceiverCreateSettings: set,
		}),
		collectorMinDuration:       collectorMinDuration,
		collectorCounter:           collectorCounter,
		tenantMinDuration:          tenantMinDuration,
		tenantLastRequestAt:        make(map[string]time.Time),
		tenantCounter:              tenantCounter,
		tenantServiceMinDuration:   tenantServiceMinDuration,
		tenantServiceLastRequestAt: make(map[string]time.Time),
		tenantServiceCounter:       tenantServiceCounter,
	}
}

// Export implements the service Export traces func.
func (r *Receiver) Export(ctx context.Context,
	req otlpgrpc.TracesRequest) (otlpgrpc.TracesResponse, error) {

	td := req.Traces()

	// We need to ensure that it propagates the receiver name as a tag
	numSpans := td.SpanCount()
	if numSpans == 0 {
		return otlpgrpc.NewTracesResponse(), nil
	}

	if c, ok := client.FromGRPC(ctx); ok {
		ctx = client.NewContext(ctx, c)
	}

	if !r.floodControl(td) {
		return otlpgrpc.NewTracesResponse(), nil
	}

	ctx = r.obsrecv.StartTracesOp(ctx)
	err := r.nextConsumer.ConsumeTraces(ctx, td)
	r.obsrecv.EndTracesOp(ctx, dataFormatProtobuf, numSpans, err)

	return otlpgrpc.NewTracesResponse(), err
}

func (r *Receiver) floodControl(td pdata.Traces) (pass bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	now := time.Now()
	if now.Sub(r.collectorLastRequestAt) < r.collectorMinDuration {
		r.collectorCounter.Inc()
		return false
	}

	tenantsToUpdate := make([]string, 0)
	td.ResourceSpans().RemoveIf(func(rs pdata.ResourceSpans) bool {
		tenantValue, _ := rs.Resource().Attributes().Get(string(semconv.ServiceNamespaceKey))
		tenant := tenantValue.StringVal()

		at, found := r.tenantLastRequestAt[tenant]
		diff := now.Sub(at)
		if !found || diff >= r.tenantMinDuration {
			tenantsToUpdate = append(tenantsToUpdate, tenant)
			return false
		}

		r.tenantCounter.With(prometheus.Labels{"tenant": tenant}).Inc()
		return true
	})

	if len(tenantsToUpdate) == 0 {
		return false
	}

	pairsToUpdate := make([]string, 0)
	td.ResourceSpans().RemoveIf(func(rs pdata.ResourceSpans) bool {
		tenantValue, _ := rs.Resource().Attributes().Get(string(semconv.ServiceNamespaceKey))
		tenant := tenantValue.StringVal()
		serviceValue, _ := rs.Resource().Attributes().Get(string(semconv.ServiceNameKey))
		service := serviceValue.StringVal()
		pair := tenant + "-->>" + service

		at, found := r.tenantServiceLastRequestAt[pair]
		diff := now.Sub(at)
		if !found || diff >= r.tenantServiceMinDuration {
			pairsToUpdate = append(pairsToUpdate, pair)
			return false
		}

		r.tenantServiceCounter.With(prometheus.Labels{"tenant": tenant, "service": service}).Inc()
		return true
	})

	if len(pairsToUpdate) == 0 {
		return false
	}

	r.collectorLastRequestAt = now
	for _, upd := range tenantsToUpdate {
		r.tenantLastRequestAt[upd] = now
	}
	for _, upd := range pairsToUpdate {
		r.tenantServiceLastRequestAt[upd] = now
	}

	return true
}
