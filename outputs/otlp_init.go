package outputs

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/falcosecurity/falcosidekick/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
)

const (
	OTLPinstrumentationName    = "falco"
	OTLPinstrumentationVersion = "v0.1.0"
)

func newResource() *otelresource.Resource {
	return otelresource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(OTLPinstrumentationName),
		semconv.ServiceVersion(OTLPinstrumentationVersion),
	)
}

func installExportPipeline(config *types.Configuration, ctx context.Context) (func(context.Context) error, error) {
	var client otlptrace.Client
	switch config.OTLP.Traces.CheckCert {
	case true:
		client = otlptracehttp.NewClient()
	case false:
		client = otlptracehttp.NewClient(otlptracehttp.WithInsecure())
	}

	exporter, err := otlptrace.New(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
	}

	withBatcher := sdktrace.WithBatcher(exporter)
	if config.OTLP.Traces.Synced {
		withBatcher = sdktrace.WithSyncer(exporter)
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(newResource()),
		withBatcher,
	)
	otel.SetTracerProvider(tracerProvider)

	return tracerProvider.Shutdown, nil
}

func otlpInit(config *types.Configuration) (func(), error) {
	// As config.OTLP.Traces fields may have been set by our own config (e.g. YAML),
	// we need to set SDK environment variables accordingly.
	os.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", config.OTLP.Traces.Endpoint)
	if config.OTLP.Traces.Protocol != "" {
		os.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", config.OTLP.Traces.Protocol)
	}
	if config.OTLP.Traces.Headers != "" {
		os.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", config.OTLP.Traces.Headers)
	}
	if config.OTLP.Traces.Timeout != 0 {
		os.Setenv("OTEL_EXPORTER_OTLP_TRACES_TIMEOUT", fmt.Sprintf("%d", config.OTLP.Traces.Timeout))
	}
	ctx := context.Background()
	// Registers a tracer Provider globally.
	shutdown, err := installExportPipeline(config, ctx)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	shutDownCallback := func() {
		if err := shutdown(ctx); err != nil {
			log.Println(err)
		}
	}
	return shutDownCallback, nil
}
