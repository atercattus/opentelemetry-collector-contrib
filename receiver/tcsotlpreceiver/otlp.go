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

package tcsotlpreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tcsotlpreceiver"

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenterror"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/otlpgrpc"
	"google.golang.org/grpc"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tcsotlpreceiver/internal/logs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tcsotlpreceiver/internal/metrics"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tcsotlpreceiver/internal/trace"
)

// otlpReceiver is the type that exposes Trace and Metrics reception.
type otlpReceiver struct {
	cfg        *Config
	serverGRPC *grpc.Server
	httpMux    *mux.Router
	serverHTTP *http.Server

	traceReceiver   *trace.Receiver
	metricsReceiver *metrics.Receiver
	logReceiver     *logs.Receiver
	shutdownWG      sync.WaitGroup

	settings component.ReceiverCreateSettings
}

// newOtlpReceiver just creates the OpenTelemetry receiver services. It is the caller's
// responsibility to invoke the respective Start*Reception methods as well
// as the various Stop*Reception methods to end it.
func newOtlpReceiver(cfg *Config, settings component.ReceiverCreateSettings) *otlpReceiver {
	r := &otlpReceiver{
		cfg:      cfg,
		settings: settings,
	}
	if cfg.HTTP != nil {
		r.httpMux = mux.NewRouter()
	}

	return r
}

func (r *otlpReceiver) startGRPCServer(cfg *configgrpc.GRPCServerSettings, host component.Host) error {
	r.settings.Logger.Info("Starting GRPC server on endpoint " + cfg.NetAddr.Endpoint)

	gln, err := cfg.ToListener()
	if err != nil {
		return err
	}
	r.shutdownWG.Add(1)
	go func() {
		defer r.shutdownWG.Done()

		if errGrpc := r.serverGRPC.Serve(gln); errGrpc != nil && errGrpc != grpc.ErrServerStopped {
			host.ReportFatalError(errGrpc)
		}
	}()
	return nil
}

func (r *otlpReceiver) startHTTPServer(cfg *confighttp.HTTPServerSettings, host component.Host) error {
	r.settings.Logger.Info("Starting HTTP server on endpoint " + cfg.Endpoint)
	var hln net.Listener
	hln, err := r.cfg.HTTP.ToListener()
	if err != nil {
		return err
	}
	r.shutdownWG.Add(1)
	go func() {
		defer r.shutdownWG.Done()

		if errHTTP := r.serverHTTP.Serve(hln); errHTTP != http.ErrServerClosed {
			host.ReportFatalError(errHTTP)
		}
	}()
	return nil
}

func (r *otlpReceiver) startProtocolServers(host component.Host) error {
	var err error
	if r.cfg.GRPC != nil {
		var opts []grpc.ServerOption
		opts, err = r.cfg.GRPC.ToServerOption(host, r.settings.TelemetrySettings)
		if err != nil {
			return err
		}
		r.serverGRPC = grpc.NewServer(opts...)

		if r.traceReceiver != nil {
			otlpgrpc.RegisterTracesServer(r.serverGRPC, r.traceReceiver)
		}

		if r.metricsReceiver != nil {
			otlpgrpc.RegisterMetricsServer(r.serverGRPC, r.metricsReceiver)
		}

		if r.logReceiver != nil {
			otlpgrpc.RegisterLogsServer(r.serverGRPC, r.logReceiver)
		}

		err = r.startGRPCServer(r.cfg.GRPC, host)
		if err != nil {
			return err
		}
	}
	if r.cfg.HTTP != nil {
		r.serverHTTP = r.cfg.HTTP.ToServer(
			r.httpMux,
			r.settings.TelemetrySettings,
			confighttp.WithErrorHandler(errorHandler),
		)
		err = r.startHTTPServer(r.cfg.HTTP, host)
		if err != nil {
			return err
		}
		if r.cfg.HTTP.Endpoint == defaultHTTPEndpoint {
			r.settings.Logger.Info("Setting up a second HTTP listener on legacy endpoint " + legacyHTTPEndpoint)

			// Copy the config.
			cfgLegacyHTTP := r.cfg.HTTP
			// And use the legacy endpoint.
			cfgLegacyHTTP.Endpoint = legacyHTTPEndpoint
			err = r.startHTTPServer(cfgLegacyHTTP, host)
			if err != nil {
				return err
			}
		}
	}

	return err
}

// Start runs the trace receiver on the gRPC server. Currently
// it also enables the metrics receiver too.
func (r *otlpReceiver) Start(_ context.Context, host component.Host) error {
	return r.startProtocolServers(host)
}

// Shutdown is a method to turn off receiving.
func (r *otlpReceiver) Shutdown(ctx context.Context) error {
	var err error

	if r.serverHTTP != nil {
		err = r.serverHTTP.Shutdown(ctx)
	}

	if r.serverGRPC != nil {
		r.serverGRPC.GracefulStop()
	}

	r.shutdownWG.Wait()
	return err
}

func (r *otlpReceiver) registerTraceConsumer(tc consumer.Traces) error {
	if tc == nil {
		return componenterror.ErrNilNextConsumer
	}

	collectorCounter := prometheus.NewCounter(
		prometheus.CounterOpts{Name: "collector_main_flood_counter"})
	tenantCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "collector_tenant_flood_counter"}, []string{"tenant"})
	tenantServiceCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "collector_tenant_service_flood_counter"},
		[]string{"tenant", "service"})

	prometheus.MustRegister(collectorCounter, tenantCounter, tenantServiceCounter)

	tr, err := trace.New(
		r.cfg.ID(),
		tc,
		r.settings,
		&http.Client{Timeout: 15 * time.Second},
		r.cfg.QuotaAdapterAPIURL,
		r.cfg.CollectorTPS,
		r.cfg.TenantTPS,
		r.cfg.TenantServiceTPS,
		collectorCounter,
		tenantCounter,
		tenantServiceCounter,
	)

	if err != nil {
		return err
	}

	r.traceReceiver = tr

	if r.httpMux != nil {
		r.httpMux.HandleFunc("/v1/traces", func(resp http.ResponseWriter, req *http.Request) {
			handleTraces(resp, req, r.traceReceiver, pbEncoder)
		}).Methods(http.MethodPost).Headers("Content-Type", pbContentType)
		// For backwards compatibility see https://github.com/open-telemetry/opentelemetry-collector/issues/1968
		r.httpMux.HandleFunc("/v1/trace", func(resp http.ResponseWriter, req *http.Request) {
			handleTraces(resp, req, r.traceReceiver, pbEncoder)
		}).Methods(http.MethodPost).Headers("Content-Type", pbContentType)
		r.httpMux.HandleFunc("/v1/traces", func(resp http.ResponseWriter, req *http.Request) {
			handleTraces(resp, req, r.traceReceiver, jsEncoder)
		}).Methods(http.MethodPost).Headers("Content-Type", jsonContentType)
		// For backwards compatibility see https://github.com/open-telemetry/opentelemetry-collector/issues/1968
		r.httpMux.HandleFunc("/v1/trace", func(resp http.ResponseWriter, req *http.Request) {
			handleTraces(resp, req, r.traceReceiver, jsEncoder)
		}).Methods(http.MethodPost).Headers("Content-Type", jsonContentType)
	}

	return nil
}

func (r *otlpReceiver) registerMetricsConsumer(mc consumer.Metrics) error {
	if mc == nil {
		return componenterror.ErrNilNextConsumer
	}
	r.metricsReceiver = metrics.New(r.cfg.ID(), mc, r.settings)
	if r.httpMux != nil {
		r.httpMux.HandleFunc("/v1/metrics", func(resp http.ResponseWriter, req *http.Request) {
			handleMetrics(resp, req, r.metricsReceiver, pbEncoder)
		}).Methods(http.MethodPost).Headers("Content-Type", pbContentType)
		r.httpMux.HandleFunc("/v1/metrics", func(resp http.ResponseWriter, req *http.Request) {
			handleMetrics(resp, req, r.metricsReceiver, jsEncoder)
		}).Methods(http.MethodPost).Headers("Content-Type", jsonContentType)
	}
	return nil
}

func (r *otlpReceiver) registerLogsConsumer(lc consumer.Logs) error {
	if lc == nil {
		return componenterror.ErrNilNextConsumer
	}
	r.logReceiver = logs.New(r.cfg.ID(), lc, r.settings)
	if r.httpMux != nil {
		r.httpMux.HandleFunc("/v1/logs", func(w http.ResponseWriter, req *http.Request) {
			handleLogs(w, req, r.logReceiver, pbEncoder)
		}).Methods(http.MethodPost).Headers("Content-Type", pbContentType)
		r.httpMux.HandleFunc("/v1/logs", func(w http.ResponseWriter, req *http.Request) {
			handleLogs(w, req, r.logReceiver, jsEncoder)
		}).Methods(http.MethodPost).Headers("Content-Type", jsonContentType)
	}
	return nil
}
