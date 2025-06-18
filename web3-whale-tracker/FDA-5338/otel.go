package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Forbes-Media/go-tools/log"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var (
	otelName       string
	otelEnabled    bool
	version        string
	otelSampleRate float64
	projectID      string
)

func init() {
	var err error

	otelEnabled, err = strconv.ParseBool(os.Getenv("OTEL_ENABLED"))
	if err != nil {
		log.Alert("failed to parse tracing: %v", err)
		log.Info("setting tracing to false")
		otelEnabled = false
	}

	if fromEnv := os.Getenv("OTEL_SERVICE_NAME"); fromEnv != "" {
		otelName = fromEnv
	} else {
		log.Alert("OTEL_SERVICE_NAME is not set")
		log.Alert("Setting OTEL_SERVICE_NAME to whale-tracker-api")
		otelName = "whale-tracker-api"
	}

	if fromEnv := os.Getenv("VERSION"); fromEnv != "" {
		version = fromEnv
	} else {
		log.Alert("VERSION is not set")
		log.Alert("Setting VERSION to v0.0.0")
		version = "v0.0.0"
	}

	if fromEnv := os.Getenv("OTEL_SAMPLE_RATE"); fromEnv != "" {
		otelSampleRate, err = strconv.ParseFloat(fromEnv, 64)
		if err != nil {
			log.Alert("failed to parse OTEL_SAMPLE_RATE: %v", err)
			log.Alert("Setting OTEL_SAMPLE_RATE to 0.05")
			otelSampleRate = 0.05
		}
	} else {
		log.Alert("OTEL_SAMPLE_RATE is not set")
		log.Alert("Setting OTEL_SAMPLE_RATE to 0.05")
		otelSampleRate = 0.05
	}

	if fromEnv := os.Getenv("PROJECT_ID"); fromEnv != "" {
		projectID = fromEnv
	} else {
		log.Alert("PROJECT_ID is not set")
		log.Alert("Setting PROJECT_ID to digital-assets-301018")
		projectID = "digital-assets-301018"
	}

	if otelEnabled {
		log.Info("OpenTelemetry enabled")
		log.Info("OTEL_SERVICE_NAME: %s", otelName)
		log.Info("OTEL_SAMPLE_RATE: %f", otelSampleRate)
	}

}

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	// Creates Resource for the Tracer
	resources, err := resource.New(ctx,
		// Use the GCP resource detector to detect information about the GCP platform
		resource.WithDetectors(gcp.NewDetector()),
		// Keep the default detectors
		resource.WithTelemetrySDK(),
		// Add your own custom attributes to identify your application
		resource.WithAttributes(
			semconv.ServiceNameKey.String(otelName),
			semconv.ServiceVersionKey.String(version),
		),
	)

	if err != nil {
		log.Alert("resource.New: %v", err)
	}

	client := otlptracehttp.NewClient()
	exporterHTTP, err := otlptrace.New(ctx, client)
	if err != nil {
		log.Critical("failed to create exporter: %v", err)
	}

	exporterGCP, err := texporter.New(texporter.WithProjectID(projectID))
	if err != nil {
		log.Critical("texporter.New: %v", err)
	}

	// Create a new trace provider with the exporter
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(otelSampleRate)),
		sdktrace.WithBatcher(exporterHTTP),
		sdktrace.WithBatcher(exporterGCP),
		sdktrace.WithResource(resources),
	)

	// Register the trace provider with the OpenTelemetry SDK
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	opencensus.InstallTraceBridge()

	return tp, nil
}

// Sets up the OpenTelemetry Metrics Provider
func initMetrics() {

	// Read and set the metrics check frequcency in seconds from the environment variable
	metricFequency, err := time.ParseDuration(os.Getenv("OTEL_METRIC_FREQUENCY"))
	if err != nil {
		log.Alert("failed to parse metric frequency: %v", err)
		log.Info("setting metric frequency to 10 seconds")
		metricFequency = time.Duration(10) * time.Second
	}

	// Creates Resource for the Tracer
	resources, err := resource.New(context.Background(),
		// Use the GCP resource detector to detect information about the GCP platform
		resource.WithDetectors(gcp.NewDetector()),
		// Keep the default detectors
		resource.WithTelemetrySDK(),
		// Add your own custom attributes to identify your application
		resource.WithAttributes(
			semconv.ServiceNameKey.String(otelName),
			semconv.ServiceVersionKey.String(version),
		),
	)

	if err != nil {
		log.Alert("resource.New: %v", err)
	}
	exporter, err := otlpmetrichttp.New(context.Background())
	if err != nil {
		log.Critical("failed to create exporter: %v", err)
	}

	// Register the exporter with an SDK via a periodic reader.
	read := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(metricFequency))

	provider := sdkmetric.NewMeterProvider(sdkmetric.WithResource(resources), sdkmetric.WithReader(read))
	defer func() {
		err := provider.Shutdown(context.Background())
		if err != nil {
			log.Alert("%s", err)
		}
	}()
	otel.SetMeterProvider(provider)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
	defer cancel()

	log.Info("Starting runtime instrumentation")
	err = runtime.Start(runtime.WithMinimumReadMemStatsInterval(metricFequency))
	if err != nil {
		log.Alert("%s", err)
	}

	log.Info("Starting host instrumentation")
	err = host.Start(host.WithMeterProvider(provider))
	if err != nil {
		log.Alert("%s", err)
	}
	<-ctx.Done()

	log.Info("Stopping runtime instrumentation")

}
