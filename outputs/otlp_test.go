package outputs

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/falcosecurity/falcosidekick/types"
	"github.com/samber/lo"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Need to mock three interfaces: TracerProvider, Tracer, Span
type (
	MockTracerProvider struct{}
	MockTracer         struct{}
	MockSpan           struct {
		name       string
		startOpts  []trace.SpanStartOption
		endOpts    []trace.SpanEndOption
		ctx        context.Context
		attributes map[attribute.Key]attribute.Value
	}
)

// TracerProvider interface {
func (*MockTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return &MockTracer{}
}

// Tracer interface
func (*MockTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ctx, &MockSpan{
		ctx:        ctx,
		name:       name,
		startOpts:  opts,
		attributes: make(map[attribute.Key]attribute.Value),
	}
}

// Span interface
func (*MockSpan) AddEvent(string, ...trace.EventOption)            {}
func (*MockSpan) IsRecording() bool                                { return true }
func (*MockSpan) RecordError(err error, opts ...trace.EventOption) {}
func (*MockSpan) SetName(name string)                              {}
func (*MockSpan) SetStatus(code codes.Code, description string)    {}

func (*MockSpan) TracerProvider() trace.TracerProvider { return &MockTracerProvider{} }

func (m *MockSpan) End(opts ...trace.SpanEndOption) {
	m.endOpts = opts
}

func (m *MockSpan) SetAttributes(kv ...attribute.KeyValue) {
	for _, k := range kv {
		m.attributes[k.Key] = k.Value
	}
}

func (m *MockSpan) SpanContext() trace.SpanContext {
	return trace.SpanContextFromContext(m.ctx)
}

func MockGetTracerProvider() trace.TracerProvider {
	return &MockTracerProvider{}
}

func startOptIn(opt trace.SpanStartOption, opts []trace.SpanStartOption) bool {
	res := lo.Filter(opts, func(o trace.SpanStartOption, index int) bool {
		return o == opt
	})
	return len(res) == 1
}

func endOptIn(opt trace.SpanEndOption, opts []trace.SpanEndOption) bool {
	res := lo.Filter(opts, func(o trace.SpanEndOption, index int) bool {
		return o == opt
	})
	return len(res) == 1
}

func TestOtlpNewTrace(t *testing.T) {
	getTracerProvider = MockGetTracerProvider

	cases := []struct {
		msg            string
		fp             types.FalcoPayload
		config         types.Configuration
		expectedHash   string
		expectedRandom bool
		actualTraceID  trace.TraceID // save traceID for below cross-cases comparison
	}{
		{
			msg: "#1 Payload with Kubernetes namespace and pod names",
			fp: types.FalcoPayload{
				OutputFields: map[string]interface{}{
					"k8s.ns.name":  "my-ns",
					"k8s.pod.name": "my-pod-name",
					"container.id": "42",
					"evt.hostname": "localhost",
				},
			},
			config: types.Configuration{
				Debug: true,
				OTLP: types.OTLPOutputConfig{
					Traces: types.OTLPTraces{
						Duration: 1000,
					},
				},
			},
			expectedHash: "087e7ab4196a3c4801e3c23bc1406163",
		},
		{
			msg: "#2 Payload with container ID",
			fp: types.FalcoPayload{
				OutputFields: map[string]interface{}{
					"container.id": "42",
					"evt.hostname": "localhost",
				},
			},
			config: types.Configuration{
				Debug: true,
				OTLP: types.OTLPOutputConfig{
					Traces: types.OTLPTraces{
						Duration: 1000,
					},
				},
			},
			expectedHash: "088094c785ab1be95aa073305569c06b",
		},
		{
			msg: "#3 Payload with Hostname",
			fp: types.FalcoPayload{
				OutputFields: map[string]interface{}{
					"evt.hostname": "localhost",
				},
			},
			config: types.Configuration{
				Debug: true,
				OTLP: types.OTLPOutputConfig{
					Traces: types.OTLPTraces{
						Duration: 1000,
					},
				},
			},
			expectedHash: "b96c8fbfe005d268653aef8210412f0a",
		},
	}
	for idx, c := range cases {
		var err error
		client, _ := NewClient("OTLP", "http://localhost:4317", false, false, &c.config, nil, nil, nil, nil)
		// Test newTrace()
		span, err := client.newTrace(c.fp)
		require.Nil(t, err)
		require.NotNil(t, span)

		// Verify SpanStartOption and SpanEndOption timestamps
		optStartTime := trace.WithTimestamp(c.fp.Time)
		optEndTime := trace.WithTimestamp(c.fp.Time.Add(time.Millisecond * time.Duration(c.config.OTLP.Traces.Duration)))
		require.Equal(t, startOptIn(optStartTime, (*span).(*MockSpan).startOpts), true, c.msg)
		require.Equal(t, endOptIn(optEndTime, (*span).(*MockSpan).endOpts), true, c.msg)

		// Verify span attributes
		require.Equal(t, attribute.StringSliceValue(c.fp.Tags), (*span).(*MockSpan).attributes[attribute.Key("tags")], c.msg)
		for k, v := range c.fp.OutputFields {
			require.Equal(t, attribute.StringValue(v.(string)), (*span).(*MockSpan).attributes[attribute.Key(k)], c.msg)
		}

		// Verify traceID
		// ~hack: to pass c.expectedRandom==true case, recreate fp.UUID as generateTraceID() derives from it
		traceID, err := generateTraceID(c.fp)
		require.Nil(t, err, c.msg)
		// Verify expectedHash
		require.Equal(t, c.expectedHash, traceID.String(), c.msg)

		// Verify test case expecting a random traceID (i.e. when the template rendered to "")
		c.actualTraceID = (*span).(*MockSpan).SpanContext().TraceID()
		if c.expectedRandom {
			require.NotEqual(t, traceID, c.actualTraceID, c.msg)
		} else {
			require.Equal(t, traceID, c.actualTraceID, c.msg)
		}

		// Save actualTraceID for 2nd pass comparison against other cases
		cases[idx].actualTraceID = c.actualTraceID
	}
	// 2nd pass to verify cross-case traceID comparisons (equality, difference)
}
