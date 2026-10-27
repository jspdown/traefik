package plugins

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	"github.com/samyfodil/wazy"
	"github.com/samyfodil/wazy/component"
	"github.com/samyfodil/wazy/component/binary"
	"github.com/samyfodil/wazy/imports/wasip2"
	"github.com/traefik/traefik/v3/pkg/middlewares"
)

const typeNameWasm = "wasm"

type wasmMiddlewareBuilder struct {
	code     []byte
	settings Settings
	runtime  wazy.Runtime
	cache    *component.CompileCache
}

func newWasmMiddlewareBuilder(goPath, moduleName, wasmPath string, settings Settings) (*wasmMiddlewareBuilder, error) {
	path := filepath.Join(goPath, "src", moduleName, wasmPath)

	code, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("loading Wasm binary: %w", err)
	}

	return newWasmMiddlewareBuilderFromCode(code, settings)
}

func newWasmMiddlewareBuilderFromCode(code []byte, settings Settings) (*wasmMiddlewareBuilder, error) {
	if err := checkPluginComponent(code); err != nil {
		return nil, err
	}

	ctx := context.Background()
	b := &wasmMiddlewareBuilder{
		code:     code,
		settings: settings,
		runtime:  wazy.NewRuntime(ctx),
		cache:    component.NewCompileCache(),
	}

	// Compiles the component, so that a plugin that cannot run fails at
	// startup rather than on its first request.
	inst, err := b.instantiate(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("compiling guest component: %w", err)
	}
	inst.close()

	return b, nil
}

// checkPluginComponent rejects what is not a component exporting
// wasi:http/handler@0.3, such as an http-wasm core module.
func checkPluginComponent(code []byte) error {
	c, err := binary.Decode(code)
	if err != nil {
		if errors.Is(err, binary.ErrInvalidVersion) || errors.Is(err, binary.ErrInvalidLayer) {
			return errors.New("the Wasm plugin is not a component: a Wasm plugin must be a component targeting the traefik:plugin/middleware world (wasi:http/middleware@0.3)")
		}

		return fmt.Errorf("decoding Wasm component: %w", err)
	}

	if !slices.ContainsFunc(c.Exports, func(e binary.Export) bool { return isHTTPHandler(e.Name) }) {
		return fmt.Errorf("the Wasm component does not export %s", ifaceHTTPHandler)
	}

	return nil
}

func isHTTPHandler(name string) bool {
	return strings.HasPrefix(name, "wasi:http/handler@0.3.")
}

func (b *wasmMiddlewareBuilder) newMiddleware(config map[string]any, middlewareName string) (pluginMiddleware, error) {
	cfg, err := wasmConfig(config)
	if err != nil {
		return nil, fmt.Errorf("plugin configuration: %w", err)
	}

	return &WasmMiddleware{
		middlewareName: middlewareName,
		config:         cfg,
		builder:        b,
	}, nil
}

// wasmConfig flattens the middleware configuration into the wasi:config/store
// entries: each top-level option is one key, a string value as is, any other
// value JSON encoded.
func wasmConfig(config map[string]any) (map[string]string, error) {
	cfg := make(map[string]string, len(config))
	for key, value := range config {
		if s, ok := value.(string); ok {
			cfg[key] = s
			continue
		}

		data, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encoding option %q: %w", key, err)
		}

		cfg[key] = string(data)
	}

	return cfg, nil
}

// instantiate creates a component instance with the host functions a
// traefik:plugin/middleware component imports.
func (b *wasmMiddlewareBuilder) instantiate(ctx context.Context, config map[string]string) (*pluginInstance, error) {
	host := &pluginHost{
		config:  config,
		objects: make(map[uint32]any),
	}

	opts, err := host.options()
	if err != nil {
		return nil, err
	}

	opts = append(opts, configStoreOptions(host)...)

	// The WASI 0.2 options come after these: a function that both versions
	// name the same way, such as monotonic-clock.now, keeps its WASI 0.2
	// implementation.
	wasiOpts, err := host.wasiOptions(os.Stdout, os.Stderr)
	if err != nil {
		return nil, err
	}
	opts = append(opts, wasiOpts...)

	wasiConfig := wasip2.Config{
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}

	for _, env := range b.settings.Envs {
		wasiConfig.Env = append(wasiConfig.Env, env+"="+os.Getenv(env))
	}

	if len(b.settings.Mounts) > 0 {
		fsConfig := wazy.NewFSConfig()
		for _, mount := range b.settings.Mounts {
			withDir := fsConfig.WithDirMount
			prefix, readOnly := strings.CutSuffix(mount, ":ro")
			if readOnly {
				withDir = fsConfig.WithReadOnlyDirMount
			}

			parts := strings.Split(prefix, ":")
			switch len(parts) {
			case 1:
				fsConfig = withDir(parts[0], parts[0])
			case 2:
				fsConfig = withDir(parts[0], parts[1])
			default:
				return nil, fmt.Errorf("invalid directory %q", mount)
			}
		}

		wasiConfig.FS = fsConfig
	}

	opts = append(opts, wasip2.With(wasiConfig)...)
	opts = append(opts, component.WithCompileCache(b.cache))

	inst, err := component.Instantiate(ctx, b.runtime, b.code, opts...)
	if err != nil {
		return nil, err
	}
	host.inst = inst

	export, ok := handleExport(inst)
	if !ok {
		_ = inst.Close(ctx)
		return nil, fmt.Errorf("the Wasm component does not export %s", ifaceHTTPHandler)
	}

	host.loop = component.NewLoop(inst)

	return &pluginInstance{inst: inst, host: host, loop: host.loop, export: export}, nil
}

func handleExport(inst *component.Instance) (string, bool) {
	for _, name := range inst.InstanceExports() {
		if isHTTPHandler(name) {
			return witFuncKey(name, "handle"), true
		}
	}

	return "", false
}

// configStoreOptions registers wasi:config/store over the middleware
// configuration.
func configStoreOptions(host *pluginHost) []component.Option {
	sigs, err := witSignatures()
	if err != nil {
		return nil
	}

	get := sigs[witFuncKey(ifaceConfigStore, "get")]
	getAll := sigs[witFuncKey(ifaceConfigStore, "get-all")]

	return []component.Option{
		component.WithImportCustom(ifaceConfigStore, "get", func(_ context.Context, args []component.Value) ([]component.Value, error) {
			value, ok := host.config[args[0].(string)]
			if !ok {
				return []component.Value{result(nil, nil)}, nil
			}

			return []component.Value{result(value, nil)}, nil
		}, get.Desc, get.Types.Resolver()),

		component.WithImportCustom(ifaceConfigStore, "get-all", func(_ context.Context, _ []component.Value) ([]component.Value, error) {
			keys := make([]string, 0, len(host.config))
			for key := range host.config {
				keys = append(keys, key)
			}
			slices.Sort(keys)

			entries := make([]component.Value, len(keys))
			for i, key := range keys {
				entries[i] = []component.Value{key, host.config[key]}
			}

			return []component.Value{result(entries, nil)}, nil
		}, getAll.Desc, getAll.Types.Resolver()),
	}
}

// WasmMiddleware is an HTTP handler plugin wrapper.
type WasmMiddleware struct {
	middlewareName string
	config         map[string]string
	builder        *wasmMiddlewareBuilder
}

// NewHandler creates a new HTTP handler.
func (m WasmMiddleware) NewHandler(ctx context.Context, next http.Handler) (http.Handler, error) {
	logger := middlewares.GetLogger(ctx, m.middlewareName, typeNameWasm)

	return wrapExternal(next, func(next http.Handler) (http.Handler, error) {
		inst, err := m.builder.instantiate(ctx, m.config)
		if err != nil {
			return nil, fmt.Errorf("building Wasm middleware: %w", err)
		}

		h := &wasmHandler{
			next:    next,
			builder: m.builder,
			config:  m.config,
			logger:  logger,
			inst:    inst,
		}

		// Traefik does not close the handlers of a previous configuration:
		// release the instance once nothing references the handler anymore.
		// Reference: https://github.com/traefik/traefik/issues/11119
		runtime.SetFinalizer(h, func(h *wasmHandler) { h.close() })

		return h, nil
	})
}

// wasmHandler runs every request through one component instance, each request
// an async call of its handle export: a plugin costs one linear memory, not
// one per request in flight.
type wasmHandler struct {
	next    http.Handler
	builder *wasmMiddlewareBuilder
	config  map[string]string
	logger  *zerolog.Logger

	mu   sync.Mutex
	inst *pluginInstance
}

func (h *wasmHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	inst, err := h.instance(req.Context())
	if err != nil {
		h.logger.Error().Err(err).Msg("Unable to instantiate the Wasm plugin")
		http.Error(rw, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)

		return
	}

	inst.serve(h.logger, h.next, rw, req)

	// The finalizer closes the instance: h must stay reachable while a
	// request uses it, also when the router holding it was replaced.
	runtime.KeepAlive(h)
}

// instance returns the instance that serves the requests. A trap poisons an
// instance for every request, so the first request after it gets a new one.
func (h *wasmHandler) instance(ctx context.Context) (*pluginInstance, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.inst != nil {
		select {
		case <-h.inst.loop.Done():
			h.logger.Error().Err(h.inst.loop.Err()).Msg("Wasm plugin instance stopped, starting a new one")
			h.inst.close()
			h.inst = nil
		default:
			return h.inst, nil
		}
	}

	// The instance outlives the request that starts it.
	inst, err := h.builder.instantiate(context.WithoutCancel(ctx), h.config)
	if err != nil {
		return nil, err
	}
	h.inst = inst

	return inst, nil
}

func (h *wasmHandler) close() {
	h.mu.Lock()
	inst := h.inst
	h.inst = nil
	h.mu.Unlock()

	if inst != nil {
		inst.close()
	}

	h.logger.Debug().Msg("Wasm middleware closed")
}

// pluginInstance is one component instance, its host state, and the loop that
// drives it.
type pluginInstance struct {
	inst   *component.Instance
	host   *pluginHost
	loop   *component.Loop
	export string
}

func (p *pluginInstance) close() {
	p.loop.Close()
	_ = p.inst.Close(context.Background())
}

type returnedResponse struct {
	resp *httpResponse
	err  error
}

// serve runs one request through the plugin.
func (p *pluginInstance) serve(logger *zerolog.Logger, next http.Handler, rw http.ResponseWriter, req *http.Request) {
	host := p.host
	call := newWasmCall(req, next, logger)

	returned := make(chan returnedResponse, 1)
	done := make(chan error, 1)

	posted := p.loop.Post(func() {
		handle := host.resources.NewOwn(tagRequest, host.add(call.ctx, newIncomingRequest(req)))

		onReturn := func(result []component.Value) {
			resp, err := host.takeResponse(result[0])
			if err == nil {
				host.streamResponseBody(call, resp.body)
			}

			returned <- returnedResponse{resp: resp, err: err}
		}

		if err := p.loop.Start(call.ctx, p.export, onReturn, func(err error) { done <- err }, handle); err != nil {
			done <- err
		}
	})
	if !posted {
		logger.Error().Err(p.loop.Err()).Msg("Wasm plugin instance stopped")
		http.Error(rw, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)

		return
	}

	var res returnedResponse
	var callErr error
	finished := false

	select {
	case res = <-returned:
	case callErr = <-done:
		finished = true

		select {
		case res = <-returned:
		default:
			res.err = callErr
			if res.err == nil {
				res.err = errors.New("the plugin did not return a response")
			}
		}
	case <-p.loop.Done():
		finished = true
		res.err = p.loop.Err()
	}

	if res.err != nil {
		logger.Error().Err(res.err).Msg("Wasm plugin failed")
		http.Error(rw, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	} else {
		writeResponse(logger, rw, res.resp)
	}

	if res.resp != nil {
		// Nothing reads the body anymore: unblock a plugin that writes it.
		_ = res.resp.body.reader.Close()
	}

	if !finished {
		// Once the client is gone, the cleanups below make the plugin give
		// up instead.
		select {
		case callErr = <-done:
		case <-p.loop.Done():
			callErr = p.loop.Err()
		case <-req.Context().Done():
			callErr = req.Context().Err()
		}
	}

	if callErr != nil {
		logger.Debug().Err(callErr).Msg("Wasm plugin call did not complete")
	}

	call.release()
	p.loop.Post(func() { host.endCall(call) })
}

// writeResponse sends the response the plugin returned to the client.
func writeResponse(logger *zerolog.Logger, rw http.ResponseWriter, resp *httpResponse) {
	header := rw.Header()
	for _, e := range resp.headers.entries {
		header.Add(e.name, string(e.value))
	}

	rw.WriteHeader(resp.status)

	if _, err := io.Copy(rw, resp.body.reader); err != nil {
		logger.Debug().Err(err).Msg("Unable to write the response body")
		return
	}

	if resp.body.trailers == nil {
		return
	}

	for name, values := range resp.body.trailers() {
		for _, value := range values {
			header.Add(http.TrailerPrefix+name, value)
		}
	}
}
