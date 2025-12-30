package redactor

import (
	"fmt"
	"reflect"

	"github.com/mitchellh/copystructure"
	"github.com/traefik/traefik/v2/pkg/config/dynamic"
	"github.com/traefik/traefik/v2/pkg/tls"
	"mvdan.cc/xurls/v2"
)

const (
	maskShort   = "xxxx"
	maskLarge   = maskShort + maskShort + maskShort + maskShort + maskShort + maskShort + maskShort + maskShort
	tagLoggable = "loggable"
	tagExport   = "export"
)

// PluginRedactor redacts a plugin.
type PluginRedactor interface {
	Redact(typ string, config map[string]any) (map[string]any, error)
}

// Redactor anonymizes configuration structures by masking sensitive fields.
// It uses struct tags to determine which fields should be redacted or preserved.
type Redactor struct {
	Tag             string
	RedactByDefault bool
	RedactURLs      bool
	PluginRedactor  PluginRedactor
}

// NewCredentialRemover creates a Redactor that redacts configuration fields with loggable=false struct tag.
func NewCredentialRemover() *Redactor {
	return &Redactor{Tag: tagLoggable, PluginRedactor: &defaultPluginRedactor{}}
}

// NewAnonymizer creates a Redactor that redacts configuration fields that do not have an export=true struct tag.
func NewAnonymizer() *Redactor {
	return &Redactor{Tag: tagExport, RedactByDefault: true, RedactURLs: true, PluginRedactor: &defaultPluginRedactor{}}
}

// Redact redacts sensitive fields.
// It returns the redacted copy without modifying the original configuration.
func (r *Redactor) Redact(baseConfig any) (any, error) {
	anomConfig, err := copystructure.Copy(baseConfig)
	if err != nil {
		return nil, err
	}

	val := reflect.ValueOf(anomConfig)
	if err = r.doOnStruct(val); err != nil {
		return nil, err
	}

	return anomConfig, nil
}

func (r *Redactor) doOnStruct(field reflect.Value) error {
	switch field.Kind() {
	case reflect.Ptr:
		if !field.IsNil() {
			if err := r.doOnStruct(field.Elem()); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := range field.NumField() {
			fld := field.Field(i)
			stField := field.Type().Field(i)
			if !isExported(stField) {
				continue
			}

			if stField.Tag.Get(r.Tag) == "false" || stField.Tag.Get(r.Tag) != "true" && r.RedactByDefault {
				if err := reset(fld, stField.Name); err != nil {
					return err
				}
				continue
			}

			// A struct field cannot be set it must be filled as pointer.
			if fld.Kind() == reflect.Struct {
				fldPtr := reflect.New(fld.Type())
				fldPtr.Elem().Set(fld)

				if err := r.doOnStruct(fldPtr); err != nil {
					return err
				}

				fld.Set(fldPtr.Elem())

				continue
			}

			if err := r.doOnStruct(fld); err != nil {
				return err
			}
		}
	case reflect.Map:
		for _, key := range field.MapKeys() {
			val := field.MapIndex(key)

			// If the value in the map is a PluginConf, redacts it. The key is the type of plugin.
			if val.Type().AssignableTo(reflect.TypeOf(dynamic.PluginConf{})) {
				redactedPluginConfig, err := r.PluginRedactor.Redact(key.String(), val.Interface().(dynamic.PluginConf))
				if err != nil {
					return fmt.Errorf("redacting plugin %s configuration: %w", key.String(), err)
				}

				field.SetMapIndex(key, reflect.ValueOf(redactedPluginConfig))

				continue
			}

			// A struct and a string value cannot be set, it must be filled as pointer.
			if val.Kind() == reflect.Struct || val.Kind() == reflect.String {
				valPtr := reflect.New(val.Type())
				valPtr.Elem().Set(val)

				if err := r.doOnStruct(valPtr); err != nil {
					return err
				}

				field.SetMapIndex(key, valPtr.Elem())

				continue
			}

			if err := r.doOnStruct(val); err != nil {
				return err
			}
		}
	case reflect.Slice:
		for j := range field.Len() {
			if err := r.doOnStruct(field.Index(j)); err != nil {
				return err
			}
		}
	case reflect.String:
		if field.CanSet() && r.RedactURLs {
			redacted := xurls.Relaxed().ReplaceAllString(field.String(), maskLarge)
			field.SetString(redacted)
		}
	}

	return nil
}

func reset(field reflect.Value, name string) error {
	if !field.CanSet() {
		return fmt.Errorf("cannot reset field %s", name)
	}

	switch field.Kind() {
	case reflect.Ptr:
		if !field.IsNil() {
			field.Set(reflect.Zero(field.Type()))
		}
	case reflect.Struct:
		if field.IsValid() {
			field.Set(reflect.Zero(field.Type()))
		}
	case reflect.String:
		if field.String() != "" {
			if field.Type().AssignableTo(reflect.TypeOf(tls.FileOrContent(""))) {
				field.Set(reflect.ValueOf(tls.FileOrContent(maskShort)))
			} else {
				field.Set(reflect.ValueOf(maskShort))
			}
		}
	case reflect.Map:
		if field.Len() > 0 {
			field.Set(reflect.MakeMap(field.Type()))
		}
	case reflect.Slice:
		if field.Len() > 0 {
			switch field.Type().Elem().Kind() {
			case reflect.String:
				slice := reflect.MakeSlice(field.Type(), field.Len(), field.Len())
				for j := range field.Len() {
					slice.Index(j).SetString(maskShort)
				}
				field.Set(slice)
			default:
				field.Set(reflect.MakeSlice(field.Type(), 0, 0))
			}
		}
	case reflect.Interface:
		return fmt.Errorf("reset not supported for interface type (for %s field)", name)
	default:
		// Primitive type
		field.Set(reflect.Zero(field.Type()))
	}
	return nil
}

// isExported return true is a struct field is exported, else false.
func isExported(f reflect.StructField) bool {
	if f.PkgPath != "" && !f.Anonymous {
		return false
	}
	return true
}

type defaultPluginRedactor struct{}

func (r *defaultPluginRedactor) Redact(_ string, config map[string]any) (map[string]any, error) {
	configCopy := make(map[string]any, len(config))
	for key := range config {
		configCopy[key] = struct{}{}
	}

	return configCopy, nil
}
