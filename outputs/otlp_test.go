package outputs

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/falcosecurity/falcosidekick/types"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type MockTracerProvider struct{}
type MockTracer struct{}
type MockSpan struct {
	name       string
	startTime  time.Time
	startOpts  []trace.SpanStartOption
	endOpts    []trace.SpanEndOption
	ctx        context.Context
	attributes map[attribute.Key]attribute.Value
}

func printContextInternals(ctx interface{}, inner bool) {
	contextValues := reflect.ValueOf(ctx).Elem()
	contextKeys := reflect.TypeOf(ctx).Elem()

	if !inner {
		fmt.Printf("\nFields for %s.%s\n", contextKeys.PkgPath(), contextKeys.Name())
	}

	if contextKeys.Kind() == reflect.Struct {
		for i := 0; i < contextValues.NumField(); i++ {
			reflectValue := contextValues.Field(i)
			reflectValue = reflect.NewAt(reflectValue.Type(), unsafe.Pointer(reflectValue.UnsafeAddr())).Elem()

			reflectField := contextKeys.Field(i)

			if reflectField.Name == "Context" {
				printContextInternals(reflectValue.Interface(), true)
			} else {
				fmt.Printf("field name: %+v\n", reflectField.Name)
				fmt.Printf("value: %+v\n", reflectValue.Interface())
			}
		}
	} else {
		fmt.Printf("context is empty (int)\n")
	}
}
func (*MockTracerProvider) Tracer(string, ...trace.TracerOption) trace.Tracer {
	return &MockTracer{}
}

func (*MockTracer) Start(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return ctx, &MockSpan{
		ctx:        ctx,
		name:       name,
		startOpts:  opts,
		attributes: make(map[attribute.Key]attribute.Value),
	}
}

func (*MockSpan) AddEvent(string, ...trace.EventOption) {}
func (m *MockSpan) End(opts ...trace.SpanEndOption) {
	for k, v := range m.attributes {
		fmt.Printf("%+v=%+v\n", k, v.Emit())
	}
	m.endOpts = opts
}
func (*MockSpan) IsRecording() bool                                { return true }
func (*MockSpan) RecordError(err error, opts ...trace.EventOption) {}
func (m *MockSpan) SetAttributes(kv ...attribute.KeyValue) {
	for _, k := range kv {
		m.attributes[k.Key] = k.Value
	}
}
func (*MockSpan) SetName(name string)                           {}
func (*MockSpan) SetStatus(code codes.Code, description string) {}
func (m *MockSpan) SpanContext() trace.SpanContext {
	return trace.SpanContextFromContext(m.ctx)
}
func (*MockSpan) TracerProvider() trace.TracerProvider { return &MockTracerProvider{} }

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
	containerID := "42"
	uuidStr := uuid.New().String()
	durationMs := 100
	startTime := time.Now()
	endTime := startTime.Add(time.Millisecond * time.Duration(durationMs))
	optStartTime := trace.WithTimestamp(startTime)
	optEndTime := trace.WithTimestamp(endTime)

	fp := &types.FalcoPayload{
		Time: startTime,
		Rule: "Mock Rule",
		OutputFields: map[string]interface{}{
			"priority":     "info",
			"uuid":         uuidStr,
			"source":       "falco",
			"container.id": containerID,
			"hostname":     "myhost",
			"output":       "Hook this Mock!",
		},
	}
	span := newTrace(*fp, 100)
	require.Equal(t, attribute.StringValue(containerID), (*span).(*MockSpan).attributes["container.id"])
	require.Equal(t, startOptIn(optStartTime, (*span).(*MockSpan).startOpts), true)
	require.Equal(t, endOptIn(optEndTime, (*span).(*MockSpan).endOpts), true)
	spanTraceID, _ := generateTraceID(containerID)
	require.Equal(t, (*span).(*MockSpan).SpanContext().TraceID(), spanTraceID)
}
