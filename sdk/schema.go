package sdk

import (
	pb "github.com/planx-lab/planx-proto/gen/go/planx/plugin/v4"
)

// FieldOption configures a ConfigField.
type FieldOption func(*pb.ConfigField)

// Required marks a field as required.
func Required() FieldOption { return func(f *pb.ConfigField) { f.Required = true } }

// WithDescription sets the field description.
func WithDescription(d string) FieldOption { return func(f *pb.ConfigField) { f.Description = d } }

// WithLabel sets the display label.
func WithLabel(l string) FieldOption { return func(f *pb.ConfigField) { f.Label = l } }

// WithDefault sets the field default value.
func WithDefault(v *pb.ConfigValue) FieldOption { return func(f *pb.ConfigField) { f.Default = v } }

// WithPlaceholder sets the UI placeholder.
func WithPlaceholder(p string) FieldOption { return func(f *pb.ConfigField) { f.Placeholder = p } }

// WithExample sets an example value (string form).
func WithExample(e string) FieldOption { return func(f *pb.ConfigField) { f.Example = e } }

func apply(name string, ft pb.FieldType, opts []FieldOption) *pb.ConfigField {
	f := &pb.ConfigField{Name: name, Type: ft}
	for _, o := range opts {
		o(f)
	}
	return f
}

// StringField declares a STRING field.
func StringField(name string, opts ...FieldOption) *pb.ConfigField {
	return apply(name, pb.FieldType_FIELD_TYPE_STRING, opts)
}

// IntegerField declares an INTEGER field.
func IntegerField(name string, opts ...FieldOption) *pb.ConfigField {
	return apply(name, pb.FieldType_FIELD_TYPE_INTEGER, opts)
}

// BooleanField declares a BOOLEAN field.
func BooleanField(name string, opts ...FieldOption) *pb.ConfigField {
	return apply(name, pb.FieldType_FIELD_TYPE_BOOLEAN, opts)
}

// SecretField declares a SECRET field (Designer masks it; never logged).
func SecretField(name string, opts ...FieldOption) *pb.ConfigField {
	return apply(name, pb.FieldType_FIELD_TYPE_SECRET, opts)
}

// EnumField declares an ENUM field with the allowed values.
func EnumField(name string, values []string, opts ...FieldOption) *pb.ConfigField {
	f := apply(name, pb.FieldType_FIELD_TYPE_ENUM, opts)
	f.EnumValues = values
	return f
}

// Schema assembles a ConfigSchema from fields.
func Schema(fields ...*pb.ConfigField) *pb.ConfigSchema {
	return &pb.ConfigSchema{Fields: fields}
}

// --- ConfigValue helpers (for defaults) ---

// StringValue wraps a string ConfigValue.
func StringValue(v string) *pb.ConfigValue {
	return &pb.ConfigValue{Kind: &pb.ConfigValue_StringValue{StringValue: v}}
}

// IntValue wraps an int64 ConfigValue.
func IntValue(v int64) *pb.ConfigValue {
	return &pb.ConfigValue{Kind: &pb.ConfigValue_IntValue{IntValue: v}}
}

// BoolValue wraps a bool ConfigValue. (Generated wrapper is
// ConfigValue_BoolValue{BoolValue: v} -- no trailing underscore; verified
// against schema.pb.go.)
func BoolValue(v bool) *pb.ConfigValue {
	return &pb.ConfigValue{Kind: &pb.ConfigValue_BoolValue{BoolValue: v}}
}
