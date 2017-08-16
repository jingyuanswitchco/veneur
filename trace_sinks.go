package veneur

import (
	"container/ring"
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
