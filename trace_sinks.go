package veneur

import (
	"container/ring"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/Sirupsen/logrus"
	lightstep "github.com/lightstep/lightstep-tracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stripe/veneur/ssf"
	"github.com/stripe/veneur/trace"
)

// TracerSink is a receiver of spans that handles sending those spans to some
// downstream sink. Calls to `Ingest(span)` are meant to give the sink control
// of the span, with periodic calls to flush as a signal for sinks that don't
// handle their own flushing in a separate goroutine, etc.
type TracerSink interface {
	Ingest(ssf.SSFSpan)
	Flush()
}

type DatadogTracerSink struct {
	HTTPClient *http.Client
	buffer     *ring.Ring
	mutex      *sync.Mutex
	stats      *statsd.Client
}

func NewDatadogTracerSink(config *Config, stats *statsd.Client, httpClient *http.Client) DatadogTracerSink {
	return DatadogTracerSink{
		HTTPClient: httpClient,
		buffer:     ring.New(config.SsfBufferSize), // TODO Fix this
		mutex:      &sync.Mutex{},
		stats:      stats,
	}
}

func (dd *DatadogTracerSink) Ingest(ssf.SSFSpan) {
}

// Flush signals the sink to send it's spans to their destination. For this
// sync it means we'll be making an HTTP request to send them along. We assume
// it's beneficial to performance to defer these until the normal 10s flush.
func (dd *DatadogTracerSink) Flush() {
	dd.mutex.Lock()

	ssfSpans := make([]ssf.SSFSpan, 0, dd.buffer.Len())

	dd.buffer.Do(func(t interface{}) {
		const tooEarly = 1497
		const tooLate = 1497629343000000

		if t != nil {
			ssfSpan, ok := t.(ssf.SSFSpan)
			if !ok {
				log.Error("Got an unknown object in tracing ring!")
				return
			}

			var timeErr string
			if ssfSpan.StartTimestamp < tooEarly {
				timeErr = "type:tooEarly"
			}
			if ssfSpan.StartTimestamp > tooLate {
				timeErr = "type:tooLate"
			}
			if timeErr != "" {
				s.Statsd.Incr("worker.trace.sink.timestamp_error", []string{timeErr}, 1)
			}

			if ssfSpan.Tags == nil {
				ssfSpan.Tags = make(map[string]string)
			}

			// this will overwrite tags already present on the span
			// TODO Move this to ingestion!
			for _, tag := range tags {
				ssfSpan.Tags[tag[0]] = tag[1]
			}
			ssfSpans = append(ssfSpans, ssfSpan)
		}
	})
	// TODO Reset the ring

	dd.mutex.Unlock()

	for _, span := range ssfSpans {
		// -1 is a canonical way of passing in invalid info in Go
		// so we should support that too
		parentID := span.ParentId

		// check if this is the root span
		if parentID <= 0 {
			// we need parentId to be zero for json:omitempty to work
			parentID = 0
		}

		resource := span.Tags[trace.ResourceKey]
		name := span.Tags[trace.NameKey]

		tags := map[string]string{}
		for k, v := range span.Tags {
			tags[k] = v
		}

		delete(tags, trace.NameKey)
		delete(tags, trace.ResourceKey)

		// TODO implement additional metrics
		var metrics map[string]float64

		var errorCode int64
		if span.Error {
			errorCode = 2
		}

		ddspan := &DatadogTraceSpan{
			TraceID:  span.TraceId,
			SpanID:   span.Id,
			ParentID: parentID,
			Service:  span.Service,
			Name:     name,
			Resource: resource,
			Start:    span.StartTimestamp,
			Duration: span.EndTimestamp - span.StartTimestamp,
			// TODO don't hardcode
			Type:    "http",
			Error:   errorCode,
			Metrics: metrics,
			Meta:    tags,
		}
		finalTraces = append(finalTraces, ddspan)
	}

	if len(finalTraces) != 0 {
		// this endpoint is not documented to take an array... but it does
		// another curious constraint of this endpoint is that it does not
		// support "Content-Encoding: deflate"

		err := postHelper(context.TODO(), httpClient, stats, fmt.Sprintf("%s/spans", ddTraceAddress), finalTraces, "flush_traces", false)

		if err == nil {
			log.WithField("traces", len(finalTraces)).Info("Completed flushing traces to Datadog")
		} else {
			log.WithFields(logrus.Fields{
				"traces":        len(finalTraces),
				logrus.ErrorKey: err}).Warn("Error flushing traces to Datadog")
		}
	} else {
		log.Info("No traces to flush to Datadog, skipping.")
	}
}

type LightStepTracerSink struct {
	tracer opentracing.Tracer
	stats  *statsd.Client
}

func NewLightStepTracerSink(config *Config, stats *statsd.Client) (LightStepTracerSink, error) {

	var host *url.URL
	host, err := url.Parse(config.TraceLightstepCollectorHost)
	if err != nil {
		log.WithError(err).WithField(
			"host", config.TraceLightstepCollectorHost,
		).Error("Error parsing LightStep collector URL")
		return LightStepTracerSink{}, err
	}

	port, err := strconv.Atoi(host.Port())
	if err != nil {
		port = lightstepDefaultPort
	} else {
		log.WithError(err).WithFields(logrus.Fields{
			"port":         port,
			"default_port": lightstepDefaultPort,
		}).Warn("Error parsing LightStep port, using default")
	}

	reconPeriod, err := time.ParseDuration(config.TraceLightstepReconnectPeriod)
	if err != nil {
		log.WithError(err).WithFields(logrus.Fields{
			"interval":         config.TraceLightstepReconnectPeriod,
			"default_interval": lightstepDefaultInterval,
		}).Warn("Failed to parse reconnect duration, using default.")
		reconPeriod = lightstepDefaultInterval
	}

	log.WithFields(logrus.Fields{
		"Host": host.Hostname(),
		"Port": port,
	}).Info("Dialing lightstep host")

	maxSpans := config.TraceLightstepMaximumSpans
	if maxSpans == 0 {
		maxSpans = conf.SsfBufferSize
		log.WithField("max spans", maxSpans).Info("Using default maximum spans — ssf_buffer_size — for LightStep")
	}

	lightstepTracer := lightstep.NewTracer(lightstep.Options{
		AccessToken:     config.TraceLightstepAccessToken,
		ReconnectPeriod: reconPeriod,
		Collector: lightstep.Endpoint{
			Host:      host.Hostname(),
			Port:      port,
			Plaintext: true,
		},
		UseGRPC:          true,
		MaxBufferedSpans: maxSpans,
	})

	return LightStepTracerSink{
		tracer: lightstepTracer,
		stats:  stats,
	}, nil
}

func (ls *LightStepTracerSink) Ingest(ssf.SSFSpan) {
	parentId := ssfSpan.ParentId
	if parentId <= 0 {
		parentId = 0
	}

	var errorCode int64
	if ssfSpan.Error {
		errorCode = 1
	}

	timestamp := time.Unix(ssfSpan.StartTimestamp/1e9, ssfSpan.StartTimestamp%1e9)
	sp := lightstepTracer.StartSpan(
		ssfSpan.Tags[trace.NameKey],
		opentracing.StartTime(timestamp),
		lightstep.SetTraceID(uint64(ssfSpan.TraceId)),
		lightstep.SetSpanID(uint64(ssfSpan.Id)),
		lightstep.SetParentSpanID(uint64(parentId)))

	sp.SetTag(trace.ResourceKey, ssfSpan.Tags[trace.ResourceKey])
	sp.SetTag(lightstep.ComponentNameKey, ssfSpan.Service)
	// TODO don't hardcode
	sp.SetTag("type", "http")
	sp.SetTag("error-code", errorCode)
	for k, v := range ssfSpan.Tags {
		sp.SetTag(k, v)
	}

	// TODO add metrics as tags to the span as well?

	if errorCode > 0 {
		// Note: this sets the OT-standard "error" tag, which
		// LightStep uses to flag error spans.
		ext.Error.Set(sp, true)
	}

	endTime := time.Unix(ssfSpan.EndTimestamp/1e9, ssfSpan.EndTimestamp%1e9)
	sp.FinishWithOptions(opentracing.FinishOptions{
		FinishTime: endTime,
	})
}

func (ls *LightStepTracerSink) Flush() {
	// TODO Metrics!
}
