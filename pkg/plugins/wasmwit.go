package plugins

import (
	"embed"
	"errors"
	"fmt"
	"sync"

	"github.com/samyfodil/wazy/component"
	"github.com/samyfodil/wazy/component/wit"
)

// witFS holds the traefik:plugin/middleware world a Wasm plugin targets, and
// the WASI packages it depends on.
//
//go:embed wit
var witFS embed.FS

const (
	ifaceHTTPTypes   = "wasi:http/types@0.3.0"
	ifaceHTTPHandler = "wasi:http/handler@0.3.0"
	ifaceConfigStore = "wasi:config/store@0.2.0-rc.1"

	ifaceMonotonicClock = "wasi:clocks/monotonic-clock@0.3.0"
	ifaceSystemClock    = "wasi:clocks/system-clock@0.3.0"
	ifaceRandom         = "wasi:random/random@0.3.0"
	ifaceInsecure       = "wasi:random/insecure@0.3.0"
	ifaceInsecureSeed   = "wasi:random/insecure-seed@0.3.0"
	ifaceStdin          = "wasi:cli/stdin@0.3.0"
	ifaceStdout         = "wasi:cli/stdout@0.3.0"
	ifaceStderr         = "wasi:cli/stderr@0.3.0"
)

// The tags the host mints its wasi:http resource handles under. They start
// far from the small values wazy's own WASI 0.2 implementation uses.
const (
	tagFields uint32 = 1<<16 + iota
	tagRequest
	tagRequestOptions
	tagResponse
)

var witResourceTags = wit.Tags{
	{Interface: ifaceHTTPTypes, Name: "fields"}:          tagFields,
	{Interface: ifaceHTTPTypes, Name: "request"}:         tagRequest,
	{Interface: ifaceHTTPTypes, Name: "request-options"}: tagRequestOptions,
	{Interface: ifaceHTTPTypes, Name: "response"}:        tagResponse,
}

// The discriminants of the WIT variants the host builds or reads, in their
// declaration order in wasi:http/types@0.3.0.
const (
	methodOther = 9

	schemeHTTP  = 0
	schemeHTTPS = 1
	schemeOther = 2

	headerErrorInvalidSyntax = 0
	headerErrorImmutable     = 2

	requestOptionsErrorImmutable = 1

	errorCodeHTTPRequestDenied = 15
	errorCodeInternalError     = 38
)

var httpMethods = []string{"GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"}

// witSignatures resolves the descriptor of every function of the interfaces
// the host implements, once.
var witSignatures = sync.OnceValues(func() (map[string]*wit.Signature, error) {
	pkg, err := wit.ParseFS(witFS, "wit")
	if err != nil {
		return nil, fmt.Errorf("parsing the plugin WIT: %w", err)
	}

	sigs := make(map[string]*wit.Signature)
	ifaces := []string{
		ifaceHTTPTypes, ifaceHTTPHandler, ifaceConfigStore,
		ifaceMonotonicClock, ifaceSystemClock,
		ifaceRandom, ifaceInsecure, ifaceInsecureSeed,
		ifaceStdin, ifaceStdout, ifaceStderr,
	}

	for _, name := range ifaces {
		iface := pkg.Interface(name)
		if iface == nil {
			return nil, fmt.Errorf("interface %s not found in the plugin WIT", name)
		}

		for _, fn := range iface.Funcs {
			sig, err := fn.Signature(witResourceTags)
			if err != nil {
				return nil, err
			}

			sigs[witFuncKey(name, fn.Name)] = sig
		}
	}

	return sigs, nil
})

func witFuncKey(iface, name string) string {
	return iface + "#" + name
}

// witFutureElem returns the element type of the future at position idx of
// the tuple the function returns, with the resolver its type indices need.
func witFutureElem(sig *wit.Signature, idx int) (component.TypeDesc, component.Resolver, error) {
	resolve := sig.Types.Resolver()

	if sig.Desc.Results.Unnamed == nil {
		return nil, nil, errors.New("function returns nothing")
	}

	tuple, ok := resolveRef(*sig.Desc.Results.Unnamed, resolve).(component.TupleDesc)
	if !ok || idx >= len(tuple.Elements) {
		return nil, nil, fmt.Errorf("function does not return a tuple of at least %d elements", idx+1)
	}

	future, ok := resolveRef(tuple.Elements[idx], resolve).(component.FutureDesc)
	if !ok || future.Element == nil {
		return nil, nil, fmt.Errorf("tuple element %d is not a future with a payload", idx)
	}

	return resolveRef(*future.Element, resolve), resolve, nil
}

// witResultFutureElem returns the element type of the future the function
// returns, with the resolver its type indices need.
func witResultFutureElem(sig *wit.Signature) (component.TypeDesc, component.Resolver, error) {
	resolve := sig.Types.Resolver()

	if sig.Desc.Results.Unnamed == nil {
		return nil, nil, errors.New("function returns nothing")
	}

	future, ok := resolveRef(*sig.Desc.Results.Unnamed, resolve).(component.FutureDesc)
	if !ok || future.Element == nil {
		return nil, nil, errors.New("function does not return a future with a payload")
	}

	return resolveRef(*future.Element, resolve), resolve, nil
}

func resolveRef(ref component.TypeRef, resolve component.Resolver) component.TypeDesc {
	if ref.TypeIndex == nil {
		return component.PrimitiveDesc{Prim: ref.Primitive}
	}

	return resolve(*ref.TypeIndex)
}
