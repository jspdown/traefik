package plugins

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand/v2"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/samyfodil/wazy/component"
)

// This file is the host side of the WASI 0.3 interfaces the
// traefik:plugin/middleware world imports besides wasi:http: clocks,
// randomness and standard I/O. A guest built for wasm32-wasip2 reaches the
// WASI 0.2 ones through wazy's wasip2 instead, for its standard library.

// cliErrorIO is the io case of wasi:cli/types.error-code.
const cliErrorIO = 0

// monotonicStart is the initial value of the monotonic clock marks.
var monotonicStart = time.Now()

func (h *pluginHost) wasiOptions(stdout, stderr io.Writer) ([]component.Option, error) {
	sigs, err := witSignatures()
	if err != nil {
		return nil, err
	}

	h.cliResultElem, h.cliResultResolve, err = witResultFutureElem(sigs[witFuncKey(ifaceStdout, "write-via-stream")])
	if err != nil {
		return nil, fmt.Errorf("stdout.write-via-stream: %w", err)
	}

	funcs := []struct {
		iface, name string
		fn          component.HostFunc
	}{
		{ifaceMonotonicClock, "now", monotonicNow},
		{ifaceMonotonicClock, "get-resolution", constant(uint64(1))},
		{ifaceSystemClock, "now", systemNow},
		{ifaceSystemClock, "get-resolution", constant(uint64(1))},
		{ifaceRandom, "get-random-bytes", randomBytes(func(b []byte) { _, _ = rand.Read(b) })},
		{ifaceRandom, "get-random-u64", randomU64(func(b []byte) { _, _ = rand.Read(b) })},
		{ifaceInsecure, "get-insecure-random-bytes", randomBytes(insecureRead)},
		{ifaceInsecure, "get-insecure-random-u64", randomU64(insecureRead)},
		{ifaceInsecureSeed, "get-insecure-seed", insecureSeed},
		{ifaceStdin, "read-via-stream", h.stdinRead},
		{ifaceStdout, "write-via-stream", h.writeViaStream(stdout)},
		{ifaceStderr, "write-via-stream", h.writeViaStream(stderr)},
	}

	var opts []component.Option
	for _, f := range funcs {
		sig, ok := sigs[witFuncKey(f.iface, f.name)]
		if !ok {
			return nil, fmt.Errorf("function %s not found in %s", f.name, f.iface)
		}

		opts = append(opts, component.WithImportCustom(f.iface, f.name, f.fn, sig.Desc, sig.Types.Resolver()))
	}

	waitUntil := sigs[witFuncKey(ifaceMonotonicClock, "wait-until")]
	waitFor := sigs[witFuncKey(ifaceMonotonicClock, "wait-for")]

	opts = append(opts,
		component.WithAsyncImportCustom(ifaceMonotonicClock, "wait-until", h.waitUntil, waitUntil.Desc, waitUntil.Types.Resolver()),
		component.WithAsyncImportCustom(ifaceMonotonicClock, "wait-for", h.waitFor, waitFor.Desc, waitFor.Types.Resolver()),
	)

	return opts, nil
}

func constant(v component.Value) component.HostFunc {
	return func(context.Context, []component.Value) ([]component.Value, error) {
		return []component.Value{v}, nil
	}
}

func monotonicNow(context.Context, []component.Value) ([]component.Value, error) {
	return []component.Value{uint64(time.Since(monotonicStart))}, nil
}

func systemNow(context.Context, []component.Value) ([]component.Value, error) {
	now := time.Now()

	return []component.Value{[]component.Value{now.Unix(), uint32(now.Nanosecond())}}, nil
}

func randomBytes(read func([]byte)) component.HostFunc {
	return func(_ context.Context, args []component.Value) ([]component.Value, error) {
		n, _ := args[0].(uint64)

		// Bounds what a guest can make the host allocate in one call.
		b := make([]byte, min(n, 1<<20))
		read(b)

		return []component.Value{b}, nil
	}
}

func randomU64(read func([]byte)) component.HostFunc {
	return func(context.Context, []component.Value) ([]component.Value, error) {
		var b [8]byte
		read(b[:])

		var v uint64
		for _, c := range b {
			v = v<<8 | uint64(c)
		}

		return []component.Value{v}, nil
	}
}

func insecureRead(b []byte) {
	for i := range b {
		b[i] = byte(mathrand.Uint32())
	}
}

func insecureSeed(context.Context, []component.Value) ([]component.Value, error) {
	return []component.Value{[]component.Value{mathrand.Uint64(), mathrand.Uint64()}}, nil
}

// waitUntil and waitFor resolve from a timer goroutine, once the import call
// returned (see handle).
func (h *pluginHost) waitUntil(ctx context.Context, args []component.Value, asyncCall *component.AsyncCall) error {
	when, _ := args[0].(uint64)
	d := time.Duration(when) - time.Since(monotonicStart)

	asyncCall.Defer(func() { resolveAfter(callFrom(ctx), asyncCall, d) })

	return nil
}

func (h *pluginHost) waitFor(ctx context.Context, args []component.Value, asyncCall *component.AsyncCall) error {
	d, _ := args[0].(uint64)

	asyncCall.Defer(func() { resolveAfter(callFrom(ctx), asyncCall, time.Duration(d)) })

	return nil
}

func resolveAfter(call *wasmCall, asyncCall *component.AsyncCall, d time.Duration) {
	resolver := &asyncResolver{}
	if call != nil {
		resolver = call.resolver
	}

	timer := time.AfterFunc(max(d, 0), func() { resolver.resolve(asyncCall, nil) })
	call.onRelease(func() { timer.Stop() })
}

// asyncResolver resolves the async import calls of one request from
// goroutines of their own. A plugin can return before a call completes. After
// the request, the instance can serve another one, so a late result must not
// reach it.
type asyncResolver struct {
	mu     sync.Mutex
	closed bool
}

func (r *asyncResolver) resolve(call *component.AsyncCall, results []component.Value) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}

	// wazy refuses a result once the export it was for returned.
	defer func() {
		if err := recover(); err != nil {
			log.Debug().Msgf("Dropping the result of a Wasm plugin call nobody waits for: %v", err)
		}
	}()

	call.Resolve(results)
}

func (r *asyncResolver) close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
}

func (h *pluginHost) stdinRead(context.Context, []component.Value) ([]component.Value, error) {
	writer, streamValue, err := h.inst.NewStream(component.PrimitiveDesc{Prim: "u8"})
	if err != nil {
		return nil, err
	}
	writer.Close()

	done, doneValue, err := h.inst.NewFutureOf(h.cliResultElem, h.cliResultResolve)
	if err != nil {
		return nil, err
	}
	done.Set(result(nil, nil), closeFuture(done))

	return []component.Value{[]component.Value{streamValue, doneValue}}, nil
}

func (h *pluginHost) writeViaStream(w io.Writer) component.HostFunc {
	return func(_ context.Context, args []component.Value) ([]component.Value, error) {
		stream, ok := args[0].(*component.StreamReader)
		if !ok {
			return nil, fmt.Errorf("unexpected stream %T", args[0])
		}

		done, doneValue, err := h.inst.NewFutureOf(h.cliResultElem, h.cliResultResolve)
		if err != nil {
			return nil, err
		}

		body := newStreamBody(h.loop, stream, nil, h.readTrailers, func(error) {})

		go func() {
			_, err := io.Copy(w, body)

			h.loop.Post(func() {
				v := result(nil, nil)
				if err != nil {
					v = result(nil, uint32(cliErrorIO))
				}

				done.Set(v, closeFuture(done))
			})
		}()

		return []component.Value{doneValue}, nil
	}
}
