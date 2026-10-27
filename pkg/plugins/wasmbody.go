package plugins

import (
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/samyfodil/wazy/component"
)

// bodyChunkSize is how many bytes a body pump moves per stream copy.
const bodyChunkSize = 32 * 1024

var (
	errStreamDropped = errors.New("stream dropped by the plugin")
	errBodyCanceled  = errors.New("body canceled")
)

// streamFeeder writes what src produces to a stream the guest reads. It runs
// on the goroutine that drives the instance: a read from src blocks the guest
// until it returns.
type streamFeeder struct {
	writer *component.StreamWriter
	src    io.Reader
	// done is called once, with nil once src reached EOF, errStreamDropped if
	// the guest dropped the stream, or the error src returned.
	done func(err error)

	buf      []byte
	chunk    []component.Value
	srcErr   error
	finished bool
}

func (f *streamFeeder) run() {
	if f.buf == nil {
		f.buf = make([]byte, bodyChunkSize)
	}

	for !f.finished {
		if len(f.chunk) == 0 {
			if f.srcErr != nil {
				f.finish(f.srcErr)
				return
			}

			n, err := f.src.Read(f.buf)
			f.chunk = bytesToValues(f.buf[:n])
			f.srcErr = err

			continue
		}

		// When Write completes at once, the loop continues here. When the
		// guest reads later, the callback starts the loop again.
		inline, completed := true, false
		f.writer.Write(f.chunk, func(res component.CopyResult, n uint32) {
			f.chunk = f.chunk[n:]

			switch res {
			case component.CopyDropped:
				f.finish(errStreamDropped)
				return
			case component.CopyCancelled:
				f.finish(errBodyCanceled)
				return
			}

			if inline {
				completed = true
				return
			}

			f.run()
		})
		inline = false

		if !completed {
			return
		}
	}
}

// cancel stops a feeder that waits for the guest to read.
func (f *streamFeeder) cancel() {
	if f.finished {
		return
	}

	f.writer.Cancel()
	f.finish(errBodyCanceled)
}

func (f *streamFeeder) finish(err error) {
	if f.finished {
		return
	}
	f.finished = true

	f.writer.Close()

	if errors.Is(err, io.EOF) {
		err = nil
	}
	f.done(err)
}

// streamDrainer copies a stream the guest writes to dst. It runs on the
// goroutine that drives the instance: a write to dst blocks the guest until it
// returns.
type streamDrainer struct {
	reader *component.StreamReader
	dst    io.Writer
	// done is called once: with nil once the guest closed the stream, or the
	// error that stopped the copy.
	done func(err error)

	finished bool
}

func (d *streamDrainer) run() {
	for !d.finished {
		// See streamFeeder.run.
		inline, completed := true, false
		d.reader.Read(bodyChunkSize, func(res component.CopyResult, vals []component.Value) {
			if len(vals) > 0 {
				if _, err := d.dst.Write(valuesToBytes(vals)); err != nil {
					d.finish(err)
					return
				}
			}

			switch res {
			case component.CopyDropped:
				d.finish(nil)
				return
			case component.CopyCancelled:
				d.finish(errBodyCanceled)
				return
			}

			if inline {
				completed = true
				return
			}

			d.run()
		})
		inline = false

		if !completed {
			return
		}
	}
}

// cancel stops a drainer that waits for the guest to write.
func (d *streamDrainer) cancel() {
	if d.finished {
		return
	}

	d.reader.Cancel()
	d.finish(errBodyCanceled)
}

func (d *streamDrainer) finish(err error) {
	if d.finished {
		return
	}
	d.finished = true

	d.reader.Close()
	d.done(err)
}

func bytesToValues(b []byte) []component.Value {
	vals := make([]component.Value, len(b))
	for i, c := range b {
		vals[i] = uint32(c)
	}

	return vals
}

func valuesToBytes(vals []component.Value) []byte {
	b := make([]byte, len(vals))
	for i, v := range vals {
		c, _ := v.(uint32)
		b[i] = byte(c)
	}

	return b
}

// nextResponseWriter is the http.ResponseWriter the next handler writes to.
// It hands the status and headers over as soon as the next handler commits
// them, and pipes the body to whoever consumes the response.
type nextResponseWriter struct {
	header   http.Header
	onHeader func(status int, header http.Header, body io.ReadCloser, trailers func() http.Header)

	mu          sync.Mutex
	wroteHeader bool
	trailers    http.Header

	bodyReader *io.PipeReader
	bodyWriter *io.PipeWriter
}

func newNextResponseWriter(onHeader func(status int, header http.Header, body io.ReadCloser, trailers func() http.Header)) *nextResponseWriter {
	pr, pw := io.Pipe()

	return &nextResponseWriter{
		header:     make(http.Header),
		onHeader:   onHeader,
		bodyReader: pr,
		bodyWriter: pw,
	}
}

func (w *nextResponseWriter) Header() http.Header {
	return w.header
}

func (w *nextResponseWriter) WriteHeader(status int) {
	// Informational responses are not forwarded to the plugin.
	if status >= 100 && status < 200 && status != http.StatusSwitchingProtocols {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.wroteHeader {
		return
	}
	w.wroteHeader = true

	header := w.header.Clone()
	for _, name := range header.Values("Trailer") {
		header.Del(name)
	}
	header.Del("Trailer")

	w.onHeader(status, header, w.bodyReader, w.getTrailers)
}

func (w *nextResponseWriter) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)

	return w.bodyWriter.Write(b)
}

func (w *nextResponseWriter) Flush() {
	w.WriteHeader(http.StatusOK)
}

func (w *nextResponseWriter) getTrailers() http.Header {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.trailers
}

// finish ends the response once the next handler returned.
func (w *nextResponseWriter) finish() {
	w.WriteHeader(http.StatusOK)

	trailers := make(http.Header)
	for _, name := range w.header.Values("Trailer") {
		if values := w.header.Values(name); len(values) > 0 {
			trailers[http.CanonicalHeaderKey(name)] = values
		}
	}
	for name, values := range w.header {
		if trailer, ok := cutTrailerPrefix(name); ok {
			trailers[trailer] = values
		}
	}

	w.mu.Lock()
	w.trailers = trailers
	w.mu.Unlock()

	_ = w.bodyWriter.Close()
}

// fail ends the response of a next handler that panicked.
func (w *nextResponseWriter) fail() {
	w.WriteHeader(http.StatusInternalServerError)

	_ = w.bodyWriter.CloseWithError(errors.New("next handler panicked"))
}

// abort unblocks a next handler that writes a body nobody reads.
func (w *nextResponseWriter) abort() {
	_ = w.bodyReader.CloseWithError(errBodyCanceled)
}

func cutTrailerPrefix(name string) (string, bool) {
	if len(name) <= len(http.TrailerPrefix) || name[:len(http.TrailerPrefix)] != http.TrailerPrefix {
		return "", false
	}

	return http.CanonicalHeaderKey(name[len(http.TrailerPrefix):]), true
}
