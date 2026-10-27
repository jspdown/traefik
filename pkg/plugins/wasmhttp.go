package plugins

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/samyfodil/wazy/component"
	"golang.org/x/net/http/httpguts"
)

// This file is the host side of wasi:http@0.3 a traefik:plugin/middleware
// component runs against: the wasi:http/types resources, and the imported
// wasi:http/handler that forwards a request to the next handler.
//
// Every function here runs on the goroutine that drives the component
// instance. The exception is the next handler and what it writes: they run on
// their own goroutine, and give their results through AsyncCall.Resolve.

// pluginHost holds the host state of one component instance.
type pluginHost struct {
	inst      *component.Instance
	resources *component.HandleTable
	config    map[string]string

	trailersElem     component.TypeDesc
	trailersResolve  component.Resolver
	resultElem       component.TypeDesc
	resultResolve    component.Resolver
	cliResultElem    component.TypeDesc
	cliResultResolve component.Resolver

	nextRep uint32
	objects map[uint32]any

	// call is the current request, nil between requests.
	call *wasmCall
}

// wasmCall is the state of one request through the plugin.
type wasmCall struct {
	ctx      context.Context
	incoming *http.Request
	next     http.Handler
	logger   *zerolog.Logger
	resolver *asyncResolver

	// cleanups release what the request leaves behind: the bodies nobody
	// read to the end, so that whatever writes them returns.
	cleanups []func()
}

func (h *pluginHost) add(obj any) uint32 {
	h.nextRep++
	h.objects[h.nextRep] = obj

	return h.nextRep
}

func (h *pluginHost) remove(rep uint32) any {
	obj := h.objects[rep]
	delete(h.objects, rep)

	return obj
}

func getObject[T any](h *pluginHost, rep uint32) (T, error) {
	obj, ok := h.objects[rep].(T)
	if !ok {
		return obj, fmt.Errorf("unknown resource %d", rep)
	}

	return obj, nil
}

func takeObject[T any](h *pluginHost, rep uint32) (T, error) {
	obj, err := getObject[T](h, rep)
	if err != nil {
		return obj, err
	}

	delete(h.objects, rep)

	return obj, nil
}

// dropObject is the destructor of every host resource: the guest dropped the
// last handle to it.
func (h *pluginHost) dropObject(_ context.Context, rep uint32) error {
	switch obj := h.remove(rep).(type) {
	case *httpRequest:
		obj.body.close()
	case *httpResponse:
		obj.body.close()
	}

	return nil
}

// httpFields is the fields resource: headers or trailers. Entries keep their
// casing and order; names compare case-insensitively.
type httpFields struct {
	entries   []fieldEntry
	immutable bool
}

type fieldEntry struct {
	name  string
	value []byte
}

func newFieldsFromHeader(header http.Header, immutable bool) *httpFields {
	f := &httpFields{immutable: immutable}

	names := make([]string, 0, len(header))
	for name := range header {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		for _, value := range header[name] {
			f.entries = append(f.entries, fieldEntry{name: name, value: []byte(value)})
		}
	}

	return f
}

func (f *httpFields) header() http.Header {
	header := make(http.Header, len(f.entries))
	for _, e := range f.entries {
		header.Add(e.name, string(e.value))
	}

	return header
}

func (f *httpFields) values(name string) [][]byte {
	var values [][]byte
	for _, e := range f.entries {
		if strings.EqualFold(e.name, name) {
			values = append(values, e.value)
		}
	}

	return values
}

func (f *httpFields) delete(name string) {
	f.entries = slices.DeleteFunc(f.entries, func(e fieldEntry) bool {
		return strings.EqualFold(e.name, name)
	})
}

func (f *httpFields) clone() *httpFields {
	return &httpFields{entries: slices.Clone(f.entries)}
}

// httpBody is the content of a request or a response, wherever it comes from.
type httpBody struct {
	// reader is content net/http produces: the incoming request body, or what
	// the next handler writes. trailers returns its trailers once reader
	// reached EOF.
	reader   io.ReadCloser
	trailers func() http.Header

	// stream and trailersFuture are content a guest produces.
	stream         *component.StreamReader
	trailersFuture *component.FutureReader

	consumed bool
}

// close releases a body nobody consumed.
func (b *httpBody) close() {
	if b == nil || b.consumed {
		return
	}

	b.consumed = true

	if b.reader != nil {
		_ = b.reader.Close()
	}
	if b.stream != nil {
		b.stream.Close()
	}
	if b.trailersFuture != nil {
		b.trailersFuture.Close()
	}
}

type httpRequest struct {
	method        string
	pathWithQuery *string
	scheme        *string
	authority     *string
	headers       *httpFields
	options       *requestOptions
	body          *httpBody

	// transmitted is the future request.new returned. The host writes it
	// once it forwarded the request.
	transmitted *component.FutureWriter
}

func newIncomingRequest(req *http.Request) *httpRequest {
	pathWithQuery := req.URL.RequestURI()

	scheme := "http"
	if req.TLS != nil {
		scheme = "https"
	}

	authority := req.Host

	return &httpRequest{
		method:        req.Method,
		pathWithQuery: &pathWithQuery,
		scheme:        &scheme,
		authority:     &authority,
		headers:       newFieldsFromHeader(req.Header, true),
		body: &httpBody{
			reader:   req.Body,
			trailers: func() http.Header { return req.Trailer },
		},
	}
}

type httpResponse struct {
	status  int
	headers *httpFields
	body    *httpBody

	// transmitted is response.new's twin of httpRequest.transmitted.
	transmitted *component.FutureWriter
}

type requestOptions struct {
	connectTimeout      *uint64
	firstByteTimeout    *uint64
	betweenBytesTimeout *uint64
	immutable           bool
}

// options registers the wasi:http/types functions.
func (h *pluginHost) options() ([]component.Option, error) {
	sigs, err := witSignatures()
	if err != nil {
		return nil, err
	}

	h.trailersElem, h.trailersResolve, err = witFutureElem(sigs[witFuncKey(ifaceHTTPTypes, "[static]request.consume-body")], 1)
	if err != nil {
		return nil, fmt.Errorf("request.consume-body: %w", err)
	}

	h.resultElem, h.resultResolve, err = witFutureElem(sigs[witFuncKey(ifaceHTTPTypes, "[static]request.new")], 1)
	if err != nil {
		return nil, fmt.Errorf("request.new: %w", err)
	}

	funcs := map[string]component.HostFunc{
		"[constructor]fields":           h.fieldsNew,
		"[static]fields.from-list":      h.fieldsFromList,
		"[method]fields.get":            h.fieldsGet,
		"[method]fields.has":            h.fieldsHas,
		"[method]fields.set":            h.fieldsSet,
		"[method]fields.delete":         h.fieldsDelete,
		"[method]fields.get-and-delete": h.fieldsGetAndDelete,
		"[method]fields.append":         h.fieldsAppend,
		"[method]fields.copy-all":       h.fieldsCopyAll,
		"[method]fields.clone":          h.fieldsClone,

		"[static]request.new":                 h.requestNew,
		"[method]request.get-method":          h.requestGetMethod,
		"[method]request.set-method":          h.requestSetMethod,
		"[method]request.get-path-with-query": h.requestGetPathWithQuery,
		"[method]request.set-path-with-query": h.requestSetPathWithQuery,
		"[method]request.get-scheme":          h.requestGetScheme,
		"[method]request.set-scheme":          h.requestSetScheme,
		"[method]request.get-authority":       h.requestGetAuthority,
		"[method]request.set-authority":       h.requestSetAuthority,
		"[method]request.get-options":         h.requestGetOptions,
		"[method]request.get-headers":         h.requestGetHeaders,
		"[static]request.consume-body":        h.requestConsumeBody,

		"[constructor]request-options":                      h.optionsNew,
		"[method]request-options.get-connect-timeout":       optionsGetter(h, func(o *requestOptions) **uint64 { return &o.connectTimeout }),
		"[method]request-options.set-connect-timeout":       optionsSetter(h, func(o *requestOptions) **uint64 { return &o.connectTimeout }),
		"[method]request-options.get-first-byte-timeout":    optionsGetter(h, func(o *requestOptions) **uint64 { return &o.firstByteTimeout }),
		"[method]request-options.set-first-byte-timeout":    optionsSetter(h, func(o *requestOptions) **uint64 { return &o.firstByteTimeout }),
		"[method]request-options.get-between-bytes-timeout": optionsGetter(h, func(o *requestOptions) **uint64 { return &o.betweenBytesTimeout }),
		"[method]request-options.set-between-bytes-timeout": optionsSetter(h, func(o *requestOptions) **uint64 { return &o.betweenBytesTimeout }),
		"[method]request-options.clone":                     h.optionsClone,

		"[static]response.new":             h.responseNew,
		"[method]response.get-status-code": h.responseGetStatusCode,
		"[method]response.set-status-code": h.responseSetStatusCode,
		"[method]response.get-headers":     h.responseGetHeaders,
		"[static]response.consume-body":    h.responseConsumeBody,
	}

	opts := []component.Option{
		component.WithResourcesHook(func(t *component.HandleTable) { h.resources = t }),
	}

	for resource, tag := range witResourceTags {
		opts = append(opts,
			component.WithResourceTag(resource.Interface, resource.Name, tag),
			component.WithHostResourceDtor(tag, h.dropObject),
		)
	}

	for name, fn := range funcs {
		sig, ok := sigs[witFuncKey(ifaceHTTPTypes, name)]
		if !ok {
			return nil, fmt.Errorf("function %s not found in %s", name, ifaceHTTPTypes)
		}

		opts = append(opts, component.WithImportCustom(ifaceHTTPTypes, name, fn, sig.Desc, sig.Types.Resolver()))
	}

	handle := sigs[witFuncKey(ifaceHTTPHandler, "handle")]
	opts = append(opts, component.WithAsyncImportCustom(ifaceHTTPHandler, "handle", h.handle, handle.Desc, handle.Types.Resolver()))

	return opts, nil
}

func (h *pluginHost) fieldsNew(_ context.Context, _ []component.Value) ([]component.Value, error) {
	return []component.Value{h.add(&httpFields{})}, nil
}

func (h *pluginHost) fieldsFromList(_ context.Context, args []component.Value) ([]component.Value, error) {
	entries, _ := args[0].([]component.Value)

	f := &httpFields{}
	for _, entry := range entries {
		pair, _ := entry.([]component.Value)
		if len(pair) != 2 {
			return nil, errors.New("fields.from-list: malformed entry")
		}

		name, _ := pair[0].(string)
		value, err := component.ListOf[byte](pair[1])
		if err != nil {
			return nil, fmt.Errorf("fields.from-list: %w", err)
		}

		if !validField(name, value) {
			return results(nil, headerError(headerErrorInvalidSyntax)), nil
		}

		f.entries = append(f.entries, fieldEntry{name: name, value: value})
	}

	return results(component.OwnedRep(h.add(f)), nil), nil
}

func (h *pluginHost) fieldsGet(_ context.Context, args []component.Value) ([]component.Value, error) {
	f, err := getObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	values := f.values(args[1].(string))
	out := make([]component.Value, len(values))
	for i, v := range values {
		out[i] = v
	}

	return []component.Value{out}, nil
}

func (h *pluginHost) fieldsHas(_ context.Context, args []component.Value) ([]component.Value, error) {
	f, err := getObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	return []component.Value{len(f.values(args[1].(string))) > 0}, nil
}

func (h *pluginHost) fieldsSet(_ context.Context, args []component.Value) ([]component.Value, error) {
	f, err := getObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}
	if f.immutable {
		return results(nil, headerError(headerErrorImmutable)), nil
	}

	name := args[1].(string)
	list, _ := args[2].([]component.Value)

	values := make([][]byte, 0, len(list))
	for _, v := range list {
		value, err := component.ListOf[byte](v)
		if err != nil {
			return nil, fmt.Errorf("fields.set: %w", err)
		}
		if !validField(name, value) {
			return results(nil, headerError(headerErrorInvalidSyntax)), nil
		}

		values = append(values, value)
	}

	f.delete(name)
	for _, value := range values {
		f.entries = append(f.entries, fieldEntry{name: name, value: value})
	}

	return results(nil, nil), nil
}

func (h *pluginHost) fieldsDelete(_ context.Context, args []component.Value) ([]component.Value, error) {
	f, err := getObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}
	if f.immutable {
		return results(nil, headerError(headerErrorImmutable)), nil
	}

	f.delete(args[1].(string))

	return results(nil, nil), nil
}

func (h *pluginHost) fieldsGetAndDelete(_ context.Context, args []component.Value) ([]component.Value, error) {
	f, err := getObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}
	if f.immutable {
		return results(nil, headerError(headerErrorImmutable)), nil
	}

	name := args[1].(string)
	values := f.values(name)
	f.delete(name)

	out := make([]component.Value, len(values))
	for i, v := range values {
		out[i] = v
	}

	return results(out, nil), nil
}

func (h *pluginHost) fieldsAppend(_ context.Context, args []component.Value) ([]component.Value, error) {
	f, err := getObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}
	if f.immutable {
		return results(nil, headerError(headerErrorImmutable)), nil
	}

	name := args[1].(string)
	value, err := component.ListOf[byte](args[2])
	if err != nil {
		return nil, fmt.Errorf("fields.append: %w", err)
	}
	if !validField(name, value) {
		return results(nil, headerError(headerErrorInvalidSyntax)), nil
	}

	f.entries = append(f.entries, fieldEntry{name: name, value: value})

	return results(nil, nil), nil
}

func (h *pluginHost) fieldsCopyAll(_ context.Context, args []component.Value) ([]component.Value, error) {
	f, err := getObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	out := make([]component.Value, len(f.entries))
	for i, e := range f.entries {
		out[i] = []component.Value{e.name, e.value}
	}

	return []component.Value{out}, nil
}

func (h *pluginHost) fieldsClone(_ context.Context, args []component.Value) ([]component.Value, error) {
	f, err := getObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	return []component.Value{h.add(f.clone())}, nil
}

func (h *pluginHost) requestNew(_ context.Context, args []component.Value) ([]component.Value, error) {
	headers, err := takeObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, fmt.Errorf("request.new: headers: %w", err)
	}
	headers.immutable = true

	req := &httpRequest{
		method:  http.MethodGet,
		headers: headers,
		body:    h.guestBody(args[1], args[2]),
	}

	if handle, ok := args[3].(uint32); ok {
		rep, err := h.resources.TakeOwn(tagRequestOptions, handle)
		if err != nil {
			return nil, fmt.Errorf("request.new: options: %w", err)
		}

		if req.options, err = takeObject[*requestOptions](h, rep); err != nil {
			return nil, fmt.Errorf("request.new: options: %w", err)
		}
		req.options.immutable = true
	}

	transmitted, transmittedValue, err := h.inst.NewFutureOf(h.resultElem, h.resultResolve)
	if err != nil {
		return nil, fmt.Errorf("request.new: %w", err)
	}
	req.transmitted = transmitted

	return []component.Value{[]component.Value{component.OwnedRep(h.add(req)), transmittedValue}}, nil
}

// guestBody is the body a guest hands over to request.new or response.new.
func (h *pluginHost) guestBody(contents, trailers component.Value) *httpBody {
	body := &httpBody{}
	body.trailersFuture, _ = trailers.(*component.FutureReader)

	if stream, ok := contents.(*component.StreamReader); ok {
		body.stream = stream
		return body
	}

	body.reader = http.NoBody

	return body
}

func (h *pluginHost) requestGetMethod(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	if i := slices.Index(httpMethods, req.method); i >= 0 {
		return []component.Value{component.VariantValue{Disc: uint32(i)}}, nil
	}

	return []component.Value{component.VariantValue{Disc: methodOther, Payload: req.method}}, nil
}

func (h *pluginHost) requestSetMethod(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	v, _ := args[1].(component.VariantValue)

	var method string
	switch {
	case int(v.Disc) < len(httpMethods):
		method = httpMethods[v.Disc]
	case v.Disc == methodOther:
		method, _ = v.Payload.(string)
	}

	if !httpguts.ValidHeaderFieldName(method) {
		return results(nil, struct{}{}), nil
	}

	req.method = method

	return results(nil, nil), nil
}

func (h *pluginHost) requestGetPathWithQuery(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	return []component.Value{optionalString(req.pathWithQuery)}, nil
}

func (h *pluginHost) requestSetPathWithQuery(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	path, ok := args[1].(string)
	if !ok {
		req.pathWithQuery = nil
		return results(nil, nil), nil
	}

	if !validPathWithQuery(path) {
		return results(nil, struct{}{}), nil
	}

	req.pathWithQuery = &path

	return results(nil, nil), nil
}

func (h *pluginHost) requestGetScheme(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	if req.scheme == nil {
		return []component.Value{nil}, nil
	}

	switch *req.scheme {
	case "http":
		return []component.Value{component.VariantValue{Disc: schemeHTTP}}, nil
	case "https":
		return []component.Value{component.VariantValue{Disc: schemeHTTPS}}, nil
	default:
		return []component.Value{component.VariantValue{Disc: schemeOther, Payload: *req.scheme}}, nil
	}
}

func (h *pluginHost) requestSetScheme(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	v, ok := args[1].(component.VariantValue)
	if !ok {
		req.scheme = nil
		return results(nil, nil), nil
	}

	var scheme string
	switch v.Disc {
	case schemeHTTP:
		scheme = "http"
	case schemeHTTPS:
		scheme = "https"
	default:
		scheme, _ = v.Payload.(string)
		if scheme == "" || strings.ContainsAny(scheme, ":/?#") {
			return results(nil, struct{}{}), nil
		}
	}

	req.scheme = &scheme

	return results(nil, nil), nil
}

func (h *pluginHost) requestGetAuthority(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	return []component.Value{optionalString(req.authority)}, nil
}

func (h *pluginHost) requestSetAuthority(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	authority, ok := args[1].(string)
	if !ok {
		req.authority = nil
		return results(nil, nil), nil
	}

	if !httpguts.ValidHostHeader(authority) {
		return results(nil, struct{}{}), nil
	}

	req.authority = &authority

	return results(nil, nil), nil
}

func (h *pluginHost) requestGetOptions(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	if req.options == nil {
		return []component.Value{nil}, nil
	}

	options := *req.options
	options.immutable = true

	return []component.Value{component.OwnedRep(h.add(&options))}, nil
}

func (h *pluginHost) requestGetHeaders(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := getObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	return []component.Value{h.add(&httpFields{entries: req.headers.entries, immutable: true})}, nil
}

func (h *pluginHost) requestConsumeBody(_ context.Context, args []component.Value) ([]component.Value, error) {
	req, err := takeObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return nil, fmt.Errorf("request.consume-body: %w", err)
	}

	return h.consumeBody(req.body, args[1])
}

func (h *pluginHost) optionsNew(_ context.Context, _ []component.Value) ([]component.Value, error) {
	return []component.Value{h.add(&requestOptions{})}, nil
}

func optionsGetter(h *pluginHost, field func(*requestOptions) **uint64) component.HostFunc {
	return func(_ context.Context, args []component.Value) ([]component.Value, error) {
		options, err := getObject[*requestOptions](h, args[0].(uint32))
		if err != nil {
			return nil, err
		}

		if d := *field(options); d != nil {
			return []component.Value{*d}, nil
		}

		return []component.Value{nil}, nil
	}
}

func optionsSetter(h *pluginHost, field func(*requestOptions) **uint64) component.HostFunc {
	return func(_ context.Context, args []component.Value) ([]component.Value, error) {
		options, err := getObject[*requestOptions](h, args[0].(uint32))
		if err != nil {
			return nil, err
		}
		if options.immutable {
			return results(nil, component.VariantValue{Disc: requestOptionsErrorImmutable}), nil
		}

		if d, ok := args[1].(uint64); ok {
			*field(options) = &d
		} else {
			*field(options) = nil
		}

		return results(nil, nil), nil
	}
}

func (h *pluginHost) optionsClone(_ context.Context, args []component.Value) ([]component.Value, error) {
	options, err := getObject[*requestOptions](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	clone := *options
	clone.immutable = false

	return []component.Value{h.add(&clone)}, nil
}

func (h *pluginHost) responseNew(_ context.Context, args []component.Value) ([]component.Value, error) {
	headers, err := takeObject[*httpFields](h, args[0].(uint32))
	if err != nil {
		return nil, fmt.Errorf("response.new: headers: %w", err)
	}
	headers.immutable = true

	resp := &httpResponse{
		status:  http.StatusOK,
		headers: headers,
		body:    h.guestBody(args[1], args[2]),
	}

	transmitted, transmittedValue, err := h.inst.NewFutureOf(h.resultElem, h.resultResolve)
	if err != nil {
		return nil, fmt.Errorf("response.new: %w", err)
	}
	resp.transmitted = transmitted

	return []component.Value{[]component.Value{component.OwnedRep(h.add(resp)), transmittedValue}}, nil
}

func (h *pluginHost) responseGetStatusCode(_ context.Context, args []component.Value) ([]component.Value, error) {
	resp, err := getObject[*httpResponse](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	return []component.Value{uint32(resp.status)}, nil
}

func (h *pluginHost) responseSetStatusCode(_ context.Context, args []component.Value) ([]component.Value, error) {
	resp, err := getObject[*httpResponse](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	status, _ := args[1].(uint32)
	if status < 100 || status > 999 {
		return results(nil, struct{}{}), nil
	}

	resp.status = int(status)

	return results(nil, nil), nil
}

func (h *pluginHost) responseGetHeaders(_ context.Context, args []component.Value) ([]component.Value, error) {
	resp, err := getObject[*httpResponse](h, args[0].(uint32))
	if err != nil {
		return nil, err
	}

	return []component.Value{h.add(&httpFields{entries: resp.headers.entries, immutable: true})}, nil
}

func (h *pluginHost) responseConsumeBody(_ context.Context, args []component.Value) ([]component.Value, error) {
	resp, err := takeObject[*httpResponse](h, args[0].(uint32))
	if err != nil {
		return nil, fmt.Errorf("response.consume-body: %w", err)
	}

	return h.consumeBody(resp.body, args[1])
}

// consumeBody hands body over to the guest as a stream and a trailers future.
func (h *pluginHost) consumeBody(body *httpBody, res component.Value) ([]component.Value, error) {
	// The host has no error to report on the guest's side of the exchange.
	if f, ok := res.(*component.FutureReader); ok {
		f.Close()
	}

	if body.consumed {
		return nil, errors.New("body already consumed")
	}
	body.consumed = true

	if body.stream != nil {
		return []component.Value{[]component.Value{body.stream, body.trailersFuture}}, nil
	}

	writer, streamValue, err := h.inst.NewStream(component.PrimitiveDesc{Prim: "u8"})
	if err != nil {
		return nil, err
	}

	trailers, trailersValue, err := h.inst.NewFutureOf(h.trailersElem, h.trailersResolve)
	if err != nil {
		return nil, err
	}

	reader := body.reader
	h.call.cleanups = append(h.call.cleanups, func() { _ = reader.Close() })

	feeder := &streamFeeder{
		writer: writer,
		src:    reader,
		done: func(err error) {
			_ = reader.Close()

			if errors.Is(err, errStreamDropped) {
				trailers.Set(result(nil, internalError("body dropped")), closeFuture(trailers))
				return
			}
			if err != nil {
				trailers.Set(result(nil, internalError(err.Error())), closeFuture(trailers))
				return
			}

			var value component.Value
			if body.trailers != nil {
				if t := body.trailers(); len(t) > 0 {
					value = h.add(newFieldsFromHeader(t, false))
				}
			}

			trailers.Set(result(value, nil), closeFuture(trailers))
		},
	}
	h.call.cleanups = append(h.call.cleanups, feeder.cancel)
	feeder.run()

	return []component.Value{[]component.Value{streamValue, trailersValue}}, nil
}

// handle is the imported wasi:http/handler.handle: it forwards the request to
// the next handler, and resolves with its response once it wrote the headers.
func (h *pluginHost) handle(_ context.Context, args []component.Value, call *component.AsyncCall) error {
	req, err := takeObject[*httpRequest](h, args[0].(uint32))
	if err != nil {
		return fmt.Errorf("handler.handle: %w", err)
	}

	out, err := h.outgoingRequest(req)
	if err != nil {
		h.call.logger.Debug().Err(err).Msg("Rejecting the request forwarded by the plugin")

		call.Resolve([]component.Value{result(nil, component.VariantValue{Disc: errorCodeHTTPRequestDenied})})
		return nil
	}

	resp := &httpResponse{}
	respRep := h.add(resp)

	// The next handler resolves the call from its own goroutine, which
	// AsyncCall only accepts once handle returned.
	call.Defer(func() { h.forward(out, req, resp, respRep, call) })

	return nil
}

// forward runs the next handler on its own goroutine, and feeds it the body
// the guest produced.
func (h *pluginHost) forward(out *http.Request, req *httpRequest, resp *httpResponse, respRep uint32, call *component.AsyncCall) {
	var bodyWriter *io.PipeWriter
	if req.body.stream != nil {
		var bodyReader *io.PipeReader
		bodyReader, bodyWriter = io.Pipe()
		out.Body = bodyReader
	}

	resolver := h.call.resolver
	rw := newNextResponseWriter(func(status int, header http.Header, body io.ReadCloser, trailers func() http.Header) {
		resp.status = status
		resp.headers = newFieldsFromHeader(header, true)
		resp.body = &httpBody{reader: body, trailers: trailers}

		resolver.resolve(call, []component.Value{result(component.OwnedRep(respRep), nil)})
	})
	h.call.cleanups = append(h.call.cleanups, rw.abort)

	next, logger := h.call.next, h.call.logger
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Error().Msgf("Next handler panicked: %v", err)
				rw.fail()
				return
			}

			rw.finish()
		}()

		next.ServeHTTP(rw, out)

		if out.Body != nil {
			_ = out.Body.Close()
		}
	}()

	if bodyWriter == nil {
		setTransmitted(req.transmitted, nil)
		return
	}

	drainer := &streamDrainer{
		reader: req.body.stream,
		dst:    bodyWriter,
		done: func(err error) {
			if err != nil {
				_ = bodyWriter.CloseWithError(err)
				setTransmitted(req.transmitted, err)

				return
			}

			h.readTrailers(req.body.trailersFuture, func(trailers http.Header, err error) {
				maps.Copy(out.Trailer, trailers)

				_ = bodyWriter.CloseWithError(err)
				setTransmitted(req.transmitted, err)
			})
		},
	}
	h.call.cleanups = append(h.call.cleanups, drainer.cancel)
	drainer.run()
}

// outgoingRequest builds the request the next handler receives. It starts
// from the incoming request, so that the next handler keeps its context,
// connection details and TLS state.
func (h *pluginHost) outgoingRequest(req *httpRequest) (*http.Request, error) {
	out := h.call.incoming.Clone(h.call.ctx)

	out.Method = req.method
	out.Header = req.headers.header()
	out.Trailer = http.Header{}

	requestURI := "/"
	if req.pathWithQuery != nil {
		requestURI = *req.pathWithQuery
	}

	u, err := url.ParseRequestURI(requestURI)
	if err != nil {
		return nil, fmt.Errorf("invalid path with query %q: %w", requestURI, err)
	}
	out.URL.Path, out.URL.RawPath, out.URL.RawQuery = u.Path, u.RawPath, u.RawQuery
	out.RequestURI = requestURI

	if req.authority != nil {
		out.Host = *req.authority
	}

	switch {
	case req.body.stream != nil:
		// forward sets the body, from what the guest writes.
		out.Body = nil
		out.ContentLength = contentLength(out.Header)
		req.body.consumed = true

	case req.body.consumed:
		return nil, errors.New("request body already consumed")

	default:
		out.Body = req.body.reader
		req.body.consumed = true

		if req.body.reader == http.NoBody {
			out.ContentLength = 0
		} else if out.Body != h.call.incoming.Body {
			out.ContentLength = contentLength(out.Header)
		}
	}

	return out, nil
}

// readTrailers reads a trailers future a guest wrote, and calls done with
// what it holds.
func (h *pluginHost) readTrailers(future *component.FutureReader, done func(http.Header, error)) {
	if future == nil {
		done(nil, nil)
		return
	}

	future.Get(func(res component.CopyResult, v component.Value) {
		future.Close()

		if res != component.CopyCompleted {
			done(nil, nil)
			return
		}

		r, _ := v.(component.ResultValue)
		if r.IsErr {
			done(nil, errors.New("guest reported an error for the body"))
			return
		}

		rep, ok := r.Payload.(uint32)
		if !ok {
			done(nil, nil)
			return
		}

		f, err := takeObject[*httpFields](h, rep)
		if err != nil {
			done(nil, err)
			return
		}

		done(f.header(), nil)
	})
}

// takeResponse takes the response the plugin returned from its handle
// export.
func (h *pluginHost) takeResponse(v component.Value) (*httpResponse, error) {
	r, ok := v.(component.ResultValue)
	if !ok {
		return nil, fmt.Errorf("unexpected handle result %T", v)
	}

	if r.IsErr {
		return nil, fmt.Errorf("plugin returned error code %v", r.Payload)
	}

	handle, ok := r.Payload.(uint32)
	if !ok {
		return nil, fmt.Errorf("unexpected response handle %T", r.Payload)
	}

	rep, err := h.resources.TakeOwn(tagResponse, handle)
	if err != nil {
		return nil, fmt.Errorf("taking the response: %w", err)
	}

	resp, err := takeObject[*httpResponse](h, rep)
	if err != nil {
		return nil, fmt.Errorf("taking the response: %w", err)
	}

	setTransmitted(resp.transmitted, nil)

	return resp, nil
}

// streamResponseBody turns a body the guest writes into one net/http reads.
// The guest writes the body after it returned the response. The returned
// start function reads the body, and must only run once something reads
// body.reader, because the first chunk can already be in the stream.
func (h *pluginHost) streamResponseBody(body *httpBody) (start func()) {
	if body.stream == nil {
		return func() {}
	}

	pr, pw := io.Pipe()
	var trailers http.Header

	stream, trailersFuture := body.stream, body.trailersFuture
	body.stream, body.trailersFuture = nil, nil
	body.reader = pr
	body.trailers = func() http.Header { return trailers }

	drainer := &streamDrainer{
		reader: stream,
		dst:    pw,
		done: func(err error) {
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}

			h.readTrailers(trailersFuture, func(t http.Header, err error) {
				trailers = t
				_ = pw.CloseWithError(err)
			})
		},
	}
	h.call.cleanups = append(h.call.cleanups, drainer.cancel)

	return drainer.run
}

func setTransmitted(f *component.FutureWriter, err error) {
	if f == nil {
		return
	}

	v := result(nil, nil)
	if err != nil {
		v = result(nil, internalError(err.Error()))
	}

	f.Set(v, closeFuture(f))
}

func closeFuture(f *component.FutureWriter) func(component.CopyResult) {
	return func(component.CopyResult) { f.Close() }
}

func contentLength(header http.Header) int64 {
	n, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil {
		return -1
	}

	return n
}

func validPathWithQuery(path string) bool {
	_, err := url.ParseRequestURI(path)
	return err == nil
}

func validField(name string, value []byte) bool {
	return httpguts.ValidHeaderFieldName(name) && httpguts.ValidHeaderFieldValue(string(value))
}

func optionalString(s *string) component.Value {
	if s == nil {
		return nil
	}

	return *s
}

// result builds a WIT result: the error arm when err is not nil.
func result(ok, err component.Value) component.Value {
	if err != nil {
		return component.ResultValue{IsErr: true, Payload: errorPayload(err)}
	}

	return component.ResultValue{Payload: ok}
}

// results is result as the only result of a host function.
func results(ok, err component.Value) []component.Value {
	return []component.Value{result(ok, err)}
}

// errorPayload maps the struct{}{} marker of an error arm without payload,
// as in `result`, to nil.
func errorPayload(err component.Value) component.Value {
	if _, ok := err.(struct{}); ok {
		return nil
	}

	return err
}

func headerError(disc uint32) component.Value {
	return component.VariantValue{Disc: disc}
}

func internalError(msg string) component.Value {
	return component.VariantValue{Disc: errorCodeInternalError, Payload: msg}
}
