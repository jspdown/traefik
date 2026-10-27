package plugins

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/samyfodil/wazy/component"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The plugin is built from fixtures/wasihttp, see its README.
const testPluginPath = "./fixtures/wasihttp/plugin.wasm"

func TestWasmMiddleware_respond(t *testing.T) {
	t.Setenv("PLUGIN_TEST", "MY-TEST")
	t.Setenv("PLUGIN_TEST_B", "MY-TEST_B")

	testCases := []struct {
		desc        string
		getSettings func(t *testing.T) (Settings, map[string]any)
		expected    string
	}{
		{
			desc: "mounts path",
			getSettings: func(t *testing.T) (Settings, map[string]any) {
				t.Helper()

				tempDir := t.TempDir()
				filePath := filepath.Join(tempDir, "hello.txt")
				err := os.WriteFile(filePath, []byte("content_test"), 0o644)
				require.NoError(t, err)

				return Settings{Mounts: []string{tempDir}}, map[string]any{
					"mode": "respond",
					"file": filePath,
				}
			},
			expected: "content_test",
		},
		{
			desc: "mounts src to dest",
			getSettings: func(t *testing.T) (Settings, map[string]any) {
				t.Helper()

				tempDir := t.TempDir()
				err := os.WriteFile(filepath.Join(tempDir, "hello.txt"), []byte("content_test"), 0o644)
				require.NoError(t, err)

				return Settings{Mounts: []string{tempDir + ":/tmp"}}, map[string]any{
					"mode": "respond",
					"file": "/tmp/hello.txt",
				}
			},
			expected: "content_test",
		},
		{
			desc: "one env",
			getSettings: func(t *testing.T) (Settings, map[string]any) {
				t.Helper()

				envs := []string{"PLUGIN_TEST"}
				return Settings{Envs: envs}, map[string]any{
					"mode": "respond",
					"envs": envs,
				}
			},
			expected: "MY-TEST\n",
		},
		{
			desc: "two env",
			getSettings: func(t *testing.T) (Settings, map[string]any) {
				t.Helper()

				envs := []string{"PLUGIN_TEST", "PLUGIN_TEST_B"}
				return Settings{Envs: envs}, map[string]any{
					"mode": "respond",
					"envs": envs,
				}
			},
			expected: "MY-TEST\nMY-TEST_B\n",
		},
	}

	for _, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			settings, config := test.getSettings(t)

			next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				rw.WriteHeader(http.StatusTeapot)
			})
			handler := newTestWasmHandler(t, settings, config, next)

			rw := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody)

			handler.ServeHTTP(rw, req)

			assert.Equal(t, http.StatusOK, rw.Code)
			assert.Equal(t, test.expected, rw.Body.String())
		})
	}
}

func TestWasmMiddleware_next(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)

		assert.Equal(t, http.MethodPost, req.Method)
		assert.Equal(t, "/foo?bar=baz", req.URL.RequestURI())
		assert.Equal(t, "example.com", req.Host)
		assert.Equal(t, "value", req.Header.Get("X-Test"))
		assert.Equal(t, "request body", string(body))

		rw.Header().Set("X-Next", "true")
		rw.WriteHeader(http.StatusCreated)
		_, _ = rw.Write([]byte("response body"))
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "next"}, next)

	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.com/foo?bar=baz", strings.NewReader("request body"))
	req.Header.Set("X-Test", "value")

	handler.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusCreated, rw.Code)
	assert.Equal(t, "true", rw.Header().Get("X-Next"))
	assert.Equal(t, "response body", rw.Body.String())
}

func TestWasmMiddleware_headers(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)

		assert.Equal(t, http.MethodPut, req.Method)
		assert.Equal(t, "/foo?bar=baz", req.URL.RequestURI())
		assert.Equal(t, "example.com", req.Host)
		assert.Equal(t, "value", req.Header.Get("X-Test"))
		assert.Equal(t, "plugin", req.Header.Get("X-Plugin-Request"))
		assert.Equal(t, "request body", string(body))

		rw.WriteHeader(http.StatusAccepted)
		_, _ = rw.Write([]byte("response body"))
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "headers", "value": "plugin"}, next)

	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "http://example.com/foo?bar=baz", strings.NewReader("request body"))
	req.Header.Set("X-Test", "value")

	handler.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusAccepted, rw.Code)
	assert.Equal(t, "plugin", rw.Header().Get("X-Plugin-Response"))
	assert.Equal(t, "response body", rw.Body.String())
}

func TestWasmMiddleware_headers_largeBodies(t *testing.T) {
	requestBody := strings.Repeat("request ", 50_000)
	responseBody := strings.Repeat("response ", 50_000)

	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)

		assert.Equal(t, requestBody, string(body))

		_, _ = rw.Write([]byte(responseBody))
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "headers", "value": "plugin"}, next)

	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(requestBody))

	handler.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Equal(t, responseBody, rw.Body.String())
}

func TestWasmMiddleware_headers_nextIgnoresBody(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusUnauthorized)
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "headers", "value": "plugin"}, next)

	for range 3 {
		rw := httptest.NewRecorder()
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(strings.Repeat("request ", 50_000)))

		handler.ServeHTTP(rw, req)

		assert.Equal(t, http.StatusUnauthorized, rw.Code)
		assert.Equal(t, "plugin", rw.Header().Get("X-Plugin-Response"))
		assert.Empty(t, rw.Body.String())
	}
}

func TestWasmMiddleware_echo(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		t.Error("next handler called")
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "echo"}, next)

	body := strings.Repeat("0123456789", 10_000)

	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(body))

	handler.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Equal(t, body, rw.Body.String())
}

func TestWasmMiddleware_uppercase(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.Header().Set("Content-Length", "11")
		rw.Header().Set("X-Next", "true")
		rw.WriteHeader(http.StatusNotFound)
		_, _ = rw.Write([]byte("hello world"))
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "uppercase"}, next)

	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody)

	handler.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusNotFound, rw.Code)
	assert.Equal(t, "true", rw.Header().Get("X-Next"))
	assert.Equal(t, "HELLO WORLD", rw.Body.String())
}

func TestWasmMiddleware_uppercase_largeBody(t *testing.T) {
	body := strings.Repeat("hello world ", 50_000)

	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		_, _ = rw.Write([]byte(body))
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "uppercase"}, next)

	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody)

	handler.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Equal(t, strings.ToUpper(body), rw.Body.String())
}

func TestWasmMiddleware_wasi(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		t.Error("next handler called")
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "wasi"}, next)

	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody)

	handler.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusOK, rw.Code)
	assert.Equal(t, "waited=true now=true random=16 stdout=true", rw.Body.String())
}

func TestWasmMiddleware_unknownMode(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		t.Error("next handler called")
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "unknown"}, next)

	rw := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", http.NoBody)

	handler.ServeHTTP(rw, req)

	assert.Equal(t, http.StatusInternalServerError, rw.Code)
}

func TestWasmMiddleware_concurrentRequests(t *testing.T) {
	next := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(rw, req.Body)
	})
	handler := newTestWasmHandler(t, Settings{}, map[string]any{"mode": "uppercase"}, next)

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			for range 5 {
				rw := httptest.NewRecorder()
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader("hello"))

				handler.ServeHTTP(rw, req)

				assert.Equal(t, http.StatusOK, rw.Code)
				assert.Equal(t, "HELLO", rw.Body.String())
			}
		})
	}
	wg.Wait()
}

func TestCheckPluginComponent(t *testing.T) {
	code, err := os.ReadFile(testPluginPath)
	require.NoError(t, err)

	assert.NoError(t, checkPluginComponent(code))

	coreModule := []byte("\x00asm\x01\x00\x00\x00")
	assert.ErrorContains(t, checkPluginComponent(coreModule), "not a component")
}

func TestWasmConfig(t *testing.T) {
	cfg, err := wasmConfig(map[string]any{
		"string": "value",
		"list":   []string{"a", "b"},
		"map":    map[string]any{"key": 1},
		"bool":   true,
	})
	require.NoError(t, err)

	expected := map[string]string{
		"string": "value",
		"list":   `["a","b"]`,
		"map":    `{"key":1}`,
		"bool":   "true",
	}
	assert.Equal(t, expected, cfg)
}

func TestWITDiscriminants(t *testing.T) {
	sigs, err := witSignatures()
	require.NoError(t, err)

	testCases := []struct {
		fn       string
		variant  string
		expected map[string]uint32
	}{
		{
			fn:      witFuncKey(ifaceHTTPTypes, "[method]request.get-method"),
			variant: "other",
			expected: map[string]uint32{
				"get": 0, "head": 1, "post": 2, "put": 3, "delete": 4, "connect": 5,
				"options": 6, "trace": 7, "patch": 8, "other": methodOther,
			},
		},
		{
			fn:       witFuncKey(ifaceHTTPTypes, "[method]request.get-scheme"),
			variant:  "HTTPS",
			expected: map[string]uint32{"HTTP": schemeHTTP, "HTTPS": schemeHTTPS, "other": schemeOther},
		},
		{
			fn:       witFuncKey(ifaceHTTPTypes, "[method]fields.set"),
			variant:  "invalid-syntax",
			expected: map[string]uint32{"invalid-syntax": headerErrorInvalidSyntax, "immutable": headerErrorImmutable},
		},
		{
			fn:       witFuncKey(ifaceHTTPTypes, "[method]request-options.set-connect-timeout"),
			variant:  "not-supported",
			expected: map[string]uint32{"immutable": requestOptionsErrorImmutable},
		},
		{
			fn:      witFuncKey(ifaceHTTPHandler, "handle"),
			variant: "internal-error",
			expected: map[string]uint32{
				"HTTP-request-denied": errorCodeHTTPRequestDenied,
				"internal-error":      errorCodeInternalError,
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.fn, func(t *testing.T) {
			sig, ok := sigs[test.fn]
			require.True(t, ok)

			cases := variantCases(t, sig.Types.Resolver(), test.variant)
			for name, disc := range test.expected {
				assert.Equal(t, disc, cases[name], name)
			}
		})
	}

	assert.Len(t, httpMethods, methodOther)
}

// variantCases returns the discriminant of each case of the variant that has
// a case called caseName.
func variantCases(t *testing.T, resolve component.Resolver, caseName string) map[string]uint32 {
	t.Helper()

	for idx := uint32(0); resolve(idx) != nil; idx++ {
		variant, ok := resolve(idx).(component.VariantDesc)
		if !ok {
			continue
		}

		cases := make(map[string]uint32)
		for disc, c := range variant.Cases {
			cases[c.Name] = uint32(disc)
		}

		if _, ok := cases[caseName]; ok {
			return cases
		}
	}

	t.Fatalf("no variant with a case %q", caseName)

	return nil
}

func newTestWasmHandler(t *testing.T, settings Settings, config map[string]any, next http.Handler) http.Handler {
	t.Helper()

	code, err := os.ReadFile(testPluginPath)
	require.NoError(t, err)

	builder, err := newWasmMiddlewareBuilderFromCode(code, settings)
	require.NoError(t, err)

	middleware, err := builder.newMiddleware(config, "test")
	require.NoError(t, err)

	logger := zerolog.New(zerolog.NewTestWriter(t)).Level(zerolog.DebugLevel)

	handler, err := middleware.NewHandler(logger.WithContext(t.Context()), next)
	require.NoError(t, err)

	return handler
}
