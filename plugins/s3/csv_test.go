package s3

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stripe/veneur/samplers"
)

type CSVTestCase struct {
	Name     string
	DDMetric samplers.DDMetric
	Row      io.Reader
}

func CSVTestCases() []CSVTestCase {

	partition := time.Now().UTC().Format("20060102")

	return []CSVTestCase{
		{
			Name: "BasicDDMetric",
			DDMetric: samplers.DDMetric{
				Name: "a.b.c.max",
				Value: [1][2]float64{[2]float64{1476119058,
					100}},
				Tags: []string{"foo:bar",
					"baz:quz"},
				MetricType: "gauge",
				Hostname:   "globalstats",
				DeviceName: "food",
				Interval:   0,
			},
			Row: strings.NewReader(fmt.Sprintf("a.b.c.max\t{foo:bar,baz:quz}\tgauge\tglobalstats\ttestbox-c3eac9\tfood\t0\t2016-10-10 05:04:18\t100\t%s\n", partition)),
		},
		{
			// Test that we are able to handle a missing field (DeviceName)
			Name: "MissingDeviceName",
			DDMetric: samplers.DDMetric{
				Name: "a.b.c.max",
				Value: [1][2]float64{[2]float64{1476119058,
					100}},
				Tags: []string{"foo:bar",
					"baz:quz"},
				MetricType: "rate",
				Hostname:   "localhost",
				DeviceName: "",
				Interval:   10,
			},
			Row: strings.NewReader(fmt.Sprintf("a.b.c.max\t{foo:bar,baz:quz}\trate\tlocalhost\ttestbox-c3eac9\t\t10\t2016-10-10 05:04:18\t100\t%s\n", partition)),
		},
		{
			// Test that we are able to handle tags which have tab characters in them
			// by quoting the entire field
			// (tags shouldn't do this, but we should handle them properly anyway)
			Name: "TabTag",
			DDMetric: samplers.DDMetric{
				Name: "a.b.c.max",
				Value: [1][2]float64{[2]float64{1476119058,
					100}},
				Tags: []string{"foo:b\tar",
					"baz:quz"},
				MetricType: "rate",
				Hostname:   "localhost",
				DeviceName: "eniac",
				Interval:   10,
			},
			Row: strings.NewReader(fmt.Sprintf("a.b.c.max\t\"{foo:b\tar,baz:quz}\"\trate\tlocalhost\ttestbox-c3eac9\teniac\t10\t2016-10-10 05:04:18\t100\t%s\n", partition)),
		},
	}
}

func TestEncodeCSV(t *testing.T) {
	testCases := CSVTestCases()

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {

			b := &bytes.Buffer{}

			w := csv.NewWriter(b)
			w.Comma = '\t'

			tm := time.Now()
			err := EncodeDDMetricCSV(tc.DDMetric, w, &tm, "testbox-c3eac9")
			assert.NoError(t, err)

			// We need to flush or there won't actually be any data there
			w.Flush()
			assert.NoError(t, err)

			assertReadersEqual(t, tc.Row, b)
		})
	}
}

// Helper function for determining that two readers are equal
func assertReadersEqual(t *testing.T, expected io.Reader, actual io.Reader) {

	// If we can seek, ensure that we're starting at the beginning
	for _, reader := range []io.Reader{expected, actual} {
		if readerSeeker, ok := reader.(io.ReadSeeker); ok {
			readerSeeker.Seek(0, io.SeekStart)
		}
	}

	// do the lazy thing for now
	bts, err := ioutil.ReadAll(expected)
	if err != nil {
		t.Fatal(err)
	}

	bts2, err := ioutil.ReadAll(actual)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, string(bts), string(bts2))
}
