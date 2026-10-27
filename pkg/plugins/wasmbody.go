package plugins

import (
	"bytes"
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
	errLoopStopped   = errors.New("plugin instance stopped")
)

// The loop goroutine runs every request the instance serves, so a body pump
// never makes it wait for the network: a goroutine of the pump reads or
// writes, and the loop only moves the chunk between it and the guest.

// streamFeeder writes what src produces to a stream the guest reads. A
// goroutine reads src; the loop writes each chunk to the stream, and asks for
// the next chunk once the guest read it all.
type streamFeeder struct {
	loop   *component.Loop
	writer *component.StreamWriter
	src    io.Reader
	// done is called once on the loop: with nil once src reached EOF,
	// errStreamDropped if the guest dropped the stream, or the error src
	// returned.
	done func(err error)

	// more and stop go from the loop to the reading goroutine. Only one
	// chunk is in flight, so a send to more never blocks.
	more chan struct{}
	stop chan struct{}

	// Loop only.
	chunk    []component.Value
	srcErr   error
	finished bool
}

func newStreamFeeder(loop *component.Loop, writer *component.StreamWriter, src io.Reader, done func(error)) *streamFeeder {
	return &streamFeeder{
		loop:   loop,
		writer: writer,
		src:    src,
		done:   done,
		more:   make(chan struct{}, 1),
		stop:   make(chan struct{}),
	}
}

func (f *streamFeeder) start() {
	go f.read()
}

func (f *streamFeeder) read() {
	buf := make([]byte, bodyChunkSize)

	for {
		n, err := f.src.Read(buf)
		chunk := bytes.Clone(buf[:n])

		if !f.loop.Post(func() { f.write(chunk, err) }) || err != nil {
			return
		}

		select {
		case <-f.more:
		case <-f.stop:
			return
		}
	}
}

func (f *streamFeeder) write(chunk []byte, err error) {
	if f.finished {
		return
	}

	f.chunk, f.srcErr = bytesToValues(chunk), err
	f.flush()
}

func (f *streamFeeder) flush() {
	for len(f.chunk) > 0 {
		// When Write completes at once, the loop continues here. When the
		// guest reads later, the callback flushes again.
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

			f.flush()
		})
		inline = false

		if !completed || f.finished {
			return
		}
	}

	if f.finished {
		return
	}

	if f.srcErr != nil {
		f.finish(f.srcErr)
		return
	}

	f.more <- struct{}{}
}

// cancel stops a feeder, on the loop.
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

	close(f.stop)
	f.writer.Close()

	if errors.Is(err, io.EOF) {
		err = nil
	}
	f.done(err)
}

// streamBody is a stream the guest writes, as an io.ReadCloser for net/http.
// Read asks the loop for the next chunk and waits for it, so the loop never
// waits for the reader.
type streamBody struct {
	loop *component.Loop

	// Loop only.
	stream         *component.StreamReader
	trailersFuture *component.FutureReader
	readTrailers   func(*component.FutureReader, func(http.Header, error))
	// onEnd is called once on the loop, when the body ended or the reader
	// closed it.
	onEnd func(err error)
	ended bool

	// results carries the outcome of the one read in flight, so that a send
	// from the loop never blocks.
	results chan streamResult

	// Reader side.
	pending []byte
	err     error
	// trailers is written on the loop before the EOF result is sent.
	trailers http.Header

	closeOnce sync.Once
}

type streamResult struct {
	data []byte
	err  error
}

func newStreamBody(loop *component.Loop, stream *component.StreamReader, trailersFuture *component.FutureReader, readTrailers func(*component.FutureReader, func(http.Header, error)), onEnd func(error)) *streamBody {
	return &streamBody{
		loop:           loop,
		stream:         stream,
		trailersFuture: trailersFuture,
		readTrailers:   readTrailers,
		onEnd:          onEnd,
		results:        make(chan streamResult, 1),
	}
}

func (b *streamBody) Read(p []byte) (int, error) {
	if len(b.pending) == 0 {
		if b.err != nil {
			return 0, b.err
		}

		size := min(len(p), bodyChunkSize)
		if !b.loop.Post(func() { b.read(size) }) {
			b.err = errLoopStopped
			return 0, b.err
		}

		select {
		case res := <-b.results:
			b.pending, b.err = res.data, res.err
		case <-b.loop.Done():
			b.err = errLoopStopped
		}

		if len(b.pending) == 0 {
			return 0, b.err
		}
	}

	n := copy(p, b.pending)
	b.pending = b.pending[n:]

	return n, nil
}

// Close stops the body. The guest sees the stream dropped.
func (b *streamBody) Close() error {
	b.closeOnce.Do(func() {
		b.loop.Post(func() { b.end(nil, errBodyCanceled, true) })
	})

	return nil
}

// Trailers returns the trailers the guest wrote, once Read returned io.EOF.
func (b *streamBody) Trailers() http.Header {
	return b.trailers
}

// read runs on the loop.
func (b *streamBody) read(size int) {
	if b.ended {
		b.results <- streamResult{err: io.EOF}
		return
	}

	b.stream.Read(uint32(size), func(res component.CopyResult, vals []component.Value) {
		data := valuesToBytes(vals)

		// Close canceled this read: report it, as a reader may still wait.
		if b.ended {
			b.results <- streamResult{data: data, err: errBodyCanceled}
			return
		}

		switch res {
		case component.CopyCompleted:
			b.results <- streamResult{data: data}

		case component.CopyDropped:
			b.readTrailers(b.trailersFuture, func(trailers http.Header, err error) {
				if b.ended {
					b.results <- streamResult{data: data, err: errBodyCanceled}
					return
				}

				b.trailers = trailers
				b.end(data, err, false)
			})

		default:
			b.end(data, errBodyCanceled, false)
		}
	})
}

// end ends the body once, on the loop, and sends the last result unless the
// reader closed the body. What is in flight when the reader closes it gets
// canceled, and its callback reports to the reader.
func (b *streamBody) end(data []byte, err error, closed bool) {
	if b.ended {
		return
	}
	b.ended = true

	b.stream.Cancel()
	b.stream.Close()
	if closed && b.trailersFuture != nil {
		b.trailersFuture.Cancel()
		b.trailersFuture.Close()
	}

	b.onEnd(err)

	if closed {
		return
	}

	if err == nil {
		err = io.EOF
	}
	b.results <- streamResult{data: data, err: err}
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
