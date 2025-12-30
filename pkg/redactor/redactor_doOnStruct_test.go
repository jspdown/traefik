package redactor

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	traefiktls "github.com/traefik/traefik/v2/pkg/tls"
)

type Courgette struct {
	Ji string
	Ho string
}

type Tomate struct {
	Ji string
	Ho string
}

type Carotte struct {
	Name         string
	EName        string `export:"true"`
	EFName       string `export:"false"`
	Value        int
	EValue       int `export:"true"`
	EFValue      int `export:"false"`
	List         []string
	EList        []string `export:"true"`
	EFList       []string `export:"false"`
	Courgette    Courgette
	ECourgette   Courgette `export:"true"`
	EFCourgette  Courgette `export:"false"`
	Pourgette    *Courgette
	EPourgette   *Courgette `export:"true"`
	EFPourgette  *Courgette `export:"false"`
	Aubergine    map[string]string
	EAubergine   map[string]string `export:"true"`
	EFAubergine  map[string]string `export:"false"`
	SAubergine   map[string]Tomate
	ESAubergine  map[string]Tomate `export:"true"`
	EFSAubergine map[string]Tomate `export:"false"`
	PSAubergine  map[string]*Tomate
	EPAubergine  map[string]*Tomate `export:"true"`
	EFPAubergine map[string]*Tomate `export:"false"`
}

func Test_doOnStruct(t *testing.T) {
	testCase := []struct {
		name            string
		base            *Carotte
		expected        *Carotte
		redactByDefault bool
	}{
		{
			name: "primitive",
			base: &Carotte{
				Name:   "koko",
				EName:  "kiki",
				Value:  666,
				EValue: 666,
				List:   []string{"test"},
				EList:  []string{"test"},
			},
			expected: &Carotte{
				Name:   "xxxx",
				EName:  "kiki",
				EValue: 666,
				List:   []string{"xxxx"},
				EList:  []string{"test"},
			},
			redactByDefault: true,
		},
		{
			name: "primitive2",
			base: &Carotte{
				Name:    "koko",
				EFName:  "keke",
				Value:   666,
				EFValue: 777,
				List:    []string{"test"},
				EFList:  []string{"test"},
			},
			expected: &Carotte{
				Name:   "koko",
				EFName: "xxxx",
				Value:  666,
				List:   []string{"test"},
				EFList: []string{"xxxx"},
			},
			redactByDefault: false,
		},
		{
			name: "struct",
			base: &Carotte{
				Name: "koko",
				Courgette: Courgette{
					Ji: "huu",
				},
			},
			expected: &Carotte{
				Name: "xxxx",
			},
			redactByDefault: true,
		},
		{
			name: "struct2",
			base: &Carotte{
				Name:   "koko",
				EFName: "keke",
				Courgette: Courgette{
					Ji: "huu",
				},
				EFCourgette: Courgette{
					Ji: "huu",
				},
			},
			expected: &Carotte{
				Name:   "koko",
				EFName: "xxxx",
				Courgette: Courgette{
					Ji: "huu",
					Ho: "",
				},
			},
			redactByDefault: false,
		},
		{
			name: "pointer",
			base: &Carotte{
				Name: "koko",
				Pourgette: &Courgette{
					Ji: "hoo",
				},
			},
			expected: &Carotte{
				Name:      "xxxx",
				Pourgette: nil,
			},
			redactByDefault: true,
		},
		{
			name: "pointer2",
			base: &Carotte{
				Name:   "koko",
				EFName: "keke",
				Pourgette: &Courgette{
					Ji: "hoo",
				},
				EFPourgette: &Courgette{
					Ji: "hoo",
				},
			},
			expected: &Carotte{
				Name:   "koko",
				EFName: "xxxx",
				Pourgette: &Courgette{
					Ji: "hoo",
				},
				EFPourgette: nil,
			},
			redactByDefault: false,
		},
		{
			name: "export struct",
			base: &Carotte{
				Name: "koko",
				ECourgette: Courgette{
					Ji: "huu",
				},
			},
			expected: &Carotte{
				Name: "xxxx",
				ECourgette: Courgette{
					Ji: "xxxx",
				},
			},
			redactByDefault: true,
		},
		{
			name: "export struct 2",
			base: &Carotte{
				Name:   "koko",
				EFName: "keke",
				ECourgette: Courgette{
					Ji: "huu",
				},
				EFCourgette: Courgette{
					Ji: "huu",
				},
			},
			expected: &Carotte{
				Name:   "koko",
				EFName: "xxxx",
				ECourgette: Courgette{
					Ji: "huu",
				},
			},
			redactByDefault: false,
		},
		{
			name: "export pointer struct",
			base: &Carotte{
				Name: "koko",
				EPourgette: &Courgette{
					Ji: "huu",
				},
			},
			expected: &Carotte{
				Name: "xxxx",
				EPourgette: &Courgette{
					Ji: "xxxx",
				},
			},
			redactByDefault: true,
		},
		{
			name: "export pointer struct 2",
			base: &Carotte{
				Name:   "koko",
				EFName: "keke",
				EPourgette: &Courgette{
					Ji: "huu",
				},
				EFPourgette: &Courgette{
					Ji: "huu",
				},
			},
			expected: &Carotte{
				Name:   "koko",
				EFName: "xxxx",
				EPourgette: &Courgette{
					Ji: "huu",
				},
				EFPourgette: nil,
			},
			redactByDefault: false,
		},
		{
			name: "export map string/string",
			base: &Carotte{
				Name: "koko",
				EAubergine: map[string]string{
					"foo": "bar",
				},
			},
			expected: &Carotte{
				Name: "xxxx",
				EAubergine: map[string]string{
					"foo": "bar",
				},
			},
			redactByDefault: true,
		},
		{
			name: "export map string/string 2",
			base: &Carotte{
				Name:   "koko",
				EFName: "keke",
				EAubergine: map[string]string{
					"foo": "bar",
				},
				EFAubergine: map[string]string{
					"foo": "bar",
				},
			},
			expected: &Carotte{
				Name:   "koko",
				EFName: "xxxx",
				EAubergine: map[string]string{
					"foo": "bar",
				},
				EFAubergine: map[string]string{},
			},
			redactByDefault: false,
		},
		{
			name: "export map string/pointer",
			base: &Carotte{
				Name: "koko",
				EPAubergine: map[string]*Tomate{
					"foo": {
						Ji: "fdskljf",
					},
				},
			},
			expected: &Carotte{
				Name: "xxxx",
				EPAubergine: map[string]*Tomate{
					"foo": {
						Ji: "xxxx",
					},
				},
			},
			redactByDefault: true,
		},
		{
			name: "export map string/pointer 2",
			base: &Carotte{
				Name: "koko",
				EPAubergine: map[string]*Tomate{
					"foo": {
						Ji: "fdskljf",
					},
				},
				EFPAubergine: map[string]*Tomate{
					"foo": {
						Ji: "fdskljf",
					},
				},
			},
			expected: &Carotte{
				Name: "koko",
				EPAubergine: map[string]*Tomate{
					"foo": {
						Ji: "fdskljf",
					},
				},
				EFPAubergine: map[string]*Tomate{},
			},
			redactByDefault: false,
		},
		{
			name: "export map string/struct",
			base: &Carotte{
				Name: "koko",
				ESAubergine: map[string]Tomate{
					"foo": {
						Ji: "JiJiJi",
					},
				},
			},
			expected: &Carotte{
				Name: "xxxx",
				ESAubergine: map[string]Tomate{
					"foo": {
						Ji: "xxxx",
					},
				},
			},
			redactByDefault: true,
		},
		{
			name: "export map string/struct 2",
			base: &Carotte{
				Name: "koko",
				ESAubergine: map[string]Tomate{
					"foo": {
						Ji: "JiJiJi",
					},
				},
				EFSAubergine: map[string]Tomate{
					"foo": {
						Ji: "JiJiJi",
					},
				},
			},
			expected: &Carotte{
				Name: "koko",
				ESAubergine: map[string]Tomate{
					"foo": {
						Ji: "JiJiJi",
					},
				},
				EFSAubergine: map[string]Tomate{},
			},
			redactByDefault: false,
		},
	}

	for _, test := range testCase {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			anonymizer := &Redactor{
				Tag:             tagExport,
				RedactByDefault: test.redactByDefault,
			}

			val := reflect.ValueOf(test.base).Elem()
			err := anonymizer.doOnStruct(val)
			require.NoError(t, err)

			assert.Equal(t, test.expected, test.base)
		})
	}
}

func Test_defaultPluginRedactor(t *testing.T) {
	testCases := []struct {
		name     string
		input    map[string]any
		expected map[string]any
	}{
		{
			name:     "empty config",
			input:    map[string]any{},
			expected: map[string]any{},
		},
		{
			name: "simple values replaced with empty struct",
			input: map[string]any{
				"key1": "value1",
				"key2": 42,
			},
			expected: map[string]any{
				"key1": struct{}{},
				"key2": struct{}{},
			},
		},
		{
			name: "nested values replaced with empty struct",
			input: map[string]any{
				"config": map[string]any{
					"nested": "value",
				},
				"list": []string{"a", "b"},
			},
			expected: map[string]any{
				"config": struct{}{},
				"list":   struct{}{},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			redactor := &defaultPluginRedactor{}
			result, err := redactor.Redact("test-plugin", test.input)

			require.NoError(t, err)
			assert.Equal(t, test.expected, result)
		})
	}
}

type PluginMiddleware struct {
	Plugin map[string]dynamic.PluginConf `export:"true"`
}

func Test_doOnStruct_with_PluginConf(t *testing.T) {
	testCases := []struct {
		name     string
		base     *PluginMiddleware
		expected *PluginMiddleware
	}{
		{
			name: "plugin config values replaced with empty struct",
			base: &PluginMiddleware{
				Plugin: map[string]dynamic.PluginConf{
					"my-plugin": {
						"secret": "sensitive-data",
						"config": map[string]any{
							"nested": "value",
						},
					},
				},
			},
			expected: &PluginMiddleware{
				Plugin: map[string]dynamic.PluginConf{
					"my-plugin": {
						"secret": struct{}{},
						"config": struct{}{},
					},
				},
			},
		},
		{
			name: "multiple plugins redacted",
			base: &PluginMiddleware{
				Plugin: map[string]dynamic.PluginConf{
					"plugin-a": {
						"key": "value-a",
					},
					"plugin-b": {
						"key": "value-b",
					},
				},
			},
			expected: &PluginMiddleware{
				Plugin: map[string]dynamic.PluginConf{
					"plugin-a": {
						"key": struct{}{},
					},
					"plugin-b": {
						"key": struct{}{},
					},
				},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			anonymizer := &Redactor{
				Tag:             tagExport,
				RedactByDefault: true,
				PluginRedactor:  &defaultPluginRedactor{},
			}

			val := reflect.ValueOf(test.base).Elem()
			err := anonymizer.doOnStruct(val)
			require.NoError(t, err)

			assert.Equal(t, test.expected, test.base)
		})
	}
}

type URLContainer struct {
	Endpoint string `export:"true"`
	Domain   string `export:"true"`
	Plain    string `export:"true"`
}

type MapURLContainer struct {
	URLs map[string]string `export:"true"`
}

type FileOrContentContainer struct {
	CertFile traefiktls.FileOrContent `export:"true"`
}

func Test_doOnStruct_RedactURLs(t *testing.T) {
	testCases := []struct {
		name       string
		base       any
		expected   any
		redactURLs bool
	}{
		{
			name: "URL in field redacted when RedactURLs is true",
			base: &URLContainer{
				Endpoint: "https://example.com/api",
				Domain:   "sub.domain.com",
				Plain:    "hello",
			},
			expected: &URLContainer{
				Endpoint: maskLarge,
				Domain:   maskLarge,
				Plain:    "hello",
			},
			redactURLs: true,
		},
		{
			name: "URL in field preserved when RedactURLs is false",
			base: &URLContainer{
				Endpoint: "https://example.com/api",
				Domain:   "sub.domain.com",
				Plain:    "hello",
			},
			expected: &URLContainer{
				Endpoint: "https://example.com/api",
				Domain:   "sub.domain.com",
				Plain:    "hello",
			},
			redactURLs: false,
		},
		{
			name: "email address redacted when RedactURLs is true",
			base: &URLContainer{
				Endpoint: "foo@example.com",
			},
			expected: &URLContainer{
				Endpoint: maskLarge,
			},
			redactURLs: true,
		},
		{
			name: "mixed content with URLs",
			base: &URLContainer{
				Endpoint: "connect to https://api.example.com for more",
			},
			expected: &URLContainer{
				Endpoint: "connect to " + maskLarge + " for more",
			},
			redactURLs: true,
		},
		{
			name: "URL in map value redacted when RedactURLs is true",
			base: &MapURLContainer{
				URLs: map[string]string{
					"api":    "https://api.example.com",
					"static": "https://static.example.com",
					"plain":  "not-a-url",
				},
			},
			expected: &MapURLContainer{
				URLs: map[string]string{
					"api":    maskLarge,
					"static": maskLarge,
					"plain":  "not-a-url",
				},
			},
			redactURLs: true,
		},
		{
			name: "URL in map value preserved when RedactURLs is false",
			base: &MapURLContainer{
				URLs: map[string]string{
					"api": "https://api.example.com",
				},
			},
			expected: &MapURLContainer{
				URLs: map[string]string{
					"api": "https://api.example.com",
				},
			},
			redactURLs: false,
		},
		{
			name: "URL in FileOrContent field redacted when RedactURLs is true",
			base: &FileOrContentContainer{
				CertFile: "https://example.com/cert.pem",
			},
			expected: &FileOrContentContainer{
				CertFile: maskLarge,
			},
			redactURLs: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			anonymizer := &Redactor{
				Tag:             tagExport,
				RedactByDefault: true,
				RedactURLs:      test.redactURLs,
			}

			val := reflect.ValueOf(test.base).Elem()
			err := anonymizer.doOnStruct(val)
			require.NoError(t, err)

			assert.Equal(t, test.expected, test.base)
		})
	}
}
