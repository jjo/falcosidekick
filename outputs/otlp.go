package outputs

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"log"
	"strings"
	"text/template"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/falcosecurity/falcosidekick/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Unit-testing helper
var getTracerProvider = otel.GetTracerProvider

func NewOtlpTracesClient(config *types.Configuration, stats *types.Statistics, promStats *types.PromStatistics, statsdClient, dogstatsdClient *statsd.Client) (*Client, error) {
	otlpClient, err := NewClient("OTLP.Traces", config.OTLP.Traces.Endpoint, false, false, config, stats, promStats, statsdClient, dogstatsdClient)
	if err != nil {
		return nil, err
	}
	shutDownFunc, err := otlpInit(config)
	if err != nil {
		return nil, err
	}
	log.Printf("[INFO] : OTLP.Traces=%+v\n", config.OTLP.Traces)
	otlpClient.ShutDownFunc = shutDownFunc
	return otlpClient, nil
}

// newTrace returns a new Trace object.
func (c *Client) newTrace(falcopayload types.FalcoPayload) *trace.Span {
	traceID, _, err := generateTraceID(falcopayload, c.Config)
	if err != nil {
		log.Printf("[ERROR] : OLTP Traces - Error generating trace id: %v for output fields %v", err, falcopayload.OutputFields)
		return nil
	}

	startTime := falcopayload.Time
	endTime := falcopayload.Time.Add(time.Millisecond * time.Duration(c.Config.OTLP.Traces.Duration))

	sc := trace.SpanContext{}.WithTraceID(traceID)
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	tracer := getTracerProvider().Tracer("falco-event")
	_, span := tracer.Start(
		ctx,
		falcopayload.Rule,
		trace.WithTimestamp(startTime),
		trace.WithSpanKind(trace.SpanKindServer))

	span.SetAttributes(attribute.String("uuid", falcopayload.UUID))
	span.SetAttributes(attribute.String("source", falcopayload.Source))
	span.SetAttributes(attribute.String("priority", falcopayload.Priority.String()))
	span.SetAttributes(attribute.String("rule", falcopayload.Rule))
	span.SetAttributes(attribute.String("output", falcopayload.Output))
	span.SetAttributes(attribute.String("hostname", falcopayload.Hostname))
	span.SetAttributes(attribute.StringSlice("tags", falcopayload.Tags))
	for k, v := range falcopayload.OutputFields {
		span.SetAttributes(attribute.String(k, fmt.Sprintf("%v", v)))
	}
	// span.AddEvent("falco-event")
	span.End(trace.WithTimestamp(endTime))

	if c.Config.Debug {
		log.Printf("[DEBUG] : OTLP Traces - payload generated successfully for traceid=%s", span.SpanContext().TraceID())
	}

	return &span
}

// OTLPPost generates an OTLP trace _implicitly_ via newTrace() by
// calling OTEL SDK's tracer.Start() --> span.End(), i.e. no need to explicitly
// do a HTTP POST
func (c *Client) OTLPTracesPost(falcopayload types.FalcoPayload) {
	c.Stats.OTLPTraces.Add(Total, 1)

	trace := c.newTrace(falcopayload)
	if trace == nil {
		go c.CountMetric(Outputs, 1, []string{"output:otlptraces", "status:error"})
		c.Stats.OTLPTraces.Add(Error, 1)
		c.PromStats.Outputs.With(map[string]string{"destination": "otlptraces", "status": Error}).Inc()
		log.Printf("[ERROR] : OLTP Traces - Error generating trace")
		return
	}
	// Setting the success status
	go c.CountMetric(Outputs, 1, []string{"output:otlptraces", "status:ok"})
	c.Stats.OTLPTraces.Add(OK, 1)
	c.PromStats.Outputs.With(map[string]string{"destination": "otlptraces", "status": OK}).Inc()
}

const (
	templateOption       = "missingkey=zero"
	containerTemplateStr = `{{.container_id}}`
)

var (
	containerTemplate = template.Must(template.New("").Option(templateOption).Parse(containerTemplateStr))
)

func sanitizeOutputFields(falcopayload types.FalcoPayload) map[string]interface{} {
	ret := make(map[string]interface{})
	for k, v := range falcopayload.OutputFields {
		k := strings.ReplaceAll(k, ".", "_")
		ret[k] = v
	}
	return ret
}

func renderTraceIDFromTemplate(falcopayload types.FalcoPayload, config *types.Configuration) (string, string) {
	tplStr := config.OTLP.Traces.TraceIDHash
	tpl := config.OTLP.Traces.TraceIDHashTemplate
	outputFields := sanitizeOutputFields(falcopayload)
	// Default to container.id `{{.container_id}}` as templating "seed" to generate traceID.
	if tplStr == "" {
		tpl, tplStr = containerTemplate, containerTemplateStr
	}
	buf := &bytes.Buffer{}
	if err := tpl.Execute(buf, outputFields); err != nil {
		log.Printf("[WARNING] : OTLP Traces - Error expanding template: %v", err)
	}
	return buf.String(), tplStr
}

func generateTraceID(falcopayload types.FalcoPayload, config *types.Configuration) (trace.TraceID, string, error) {
	var traceID trace.TraceID
	var err error
	traceIDStr, tplStr := renderTraceIDFromTemplate(falcopayload, config)

	switch traceIDStr {
	case "":
	case "<no value>":
		// Template produced no string, derive the traceID from the payload UUID
		traceIDStr = falcopayload.UUID
	}
	// Hash the returned template- rendered string to generate a 32 character traceID
	hash := fnv.New128a()
	hash.Write([]byte(traceIDStr))
	digest := hash.Sum(nil)
	traceIDStr = hex.EncodeToString(digest[:])
	traceID, err = trace.TraceIDFromHex(traceIDStr)
	return traceID, tplStr, err
}
