package turntf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// UserMetadataSystemKeyPrefix 是系统保留 metadata key 的前缀。
const UserMetadataSystemKeyPrefix = "system."

// UserMetadataKeyVisibleToOthers 控制用户或频道是否会出现在普通用户的可见列表中。
const UserMetadataKeyVisibleToOthers = "system.visible_to_others"

// MetadataTypedValueKind 描述 HTTP metadata typed_value 的值类型。
type MetadataTypedValueKind string

const (
	MetadataTypedValueKindBytes  MetadataTypedValueKind = "bytes"
	MetadataTypedValueKindBool   MetadataTypedValueKind = "bool"
	MetadataTypedValueKindString MetadataTypedValueKind = "string"
	MetadataTypedValueKindNumber MetadataTypedValueKind = "number"
	MetadataTypedValueKindJSON   MetadataTypedValueKind = "json"
)

// MetadataTypedValue 是 HTTP metadata 的 typed_value 视图。
// 仅 HTTP JSON 接口支持该视图；WebSocket / protobuf 仍只使用原始 Value 字节。
type MetadataTypedValue struct {
	Kind        MetadataTypedValueKind `json:"kind"`
	BytesValue  *[]byte                `json:"bytes_value,omitempty"`
	BoolValue   *bool                  `json:"bool_value,omitempty"`
	StringValue *string                `json:"string_value,omitempty"`
	NumberValue *json.RawMessage       `json:"number_value,omitempty"`
	JSONValue   *json.RawMessage       `json:"json_value,omitempty"`
}

// NewMetadataTypedBytes 构造一个 bytes 类型的 typed_value。
func NewMetadataTypedBytes(value []byte) *MetadataTypedValue {
	cloned := cloneBytesOrEmpty(value)
	return &MetadataTypedValue{
		Kind:       MetadataTypedValueKindBytes,
		BytesValue: &cloned,
	}
}

// NewMetadataTypedBool 构造一个 bool 类型的 typed_value。
func NewMetadataTypedBool(value bool) *MetadataTypedValue {
	return &MetadataTypedValue{
		Kind:      MetadataTypedValueKindBool,
		BoolValue: &value,
	}
}

// NewMetadataTypedString 构造一个 string 类型的 typed_value。
func NewMetadataTypedString(value string) *MetadataTypedValue {
	return &MetadataTypedValue{
		Kind:        MetadataTypedValueKindString,
		StringValue: &value,
	}
}

// NewMetadataTypedNumber 构造一个 number 类型的 typed_value。
// raw 必须是单个合法的 JSON number，例如 `json.RawMessage("7.5")`。
func NewMetadataTypedNumber(raw json.RawMessage) *MetadataTypedValue {
	cloned := cloneRawMessageOrEmpty(raw)
	return &MetadataTypedValue{
		Kind:        MetadataTypedValueKindNumber,
		NumberValue: &cloned,
	}
}

// NewMetadataTypedJSON 构造一个 json 类型的 typed_value。
// raw 必须是单个合法的 JSON 值，例如对象、数组或 null。
func NewMetadataTypedJSON(raw json.RawMessage) *MetadataTypedValue {
	cloned := cloneRawMessageOrEmpty(raw)
	return &MetadataTypedValue{
		Kind:      MetadataTypedValueKindJSON,
		JSONValue: &cloned,
	}
}

// MetadataBoolBytes 将布尔值编码为 metadata raw bytes 语义使用的 `true` / `false`。
// 这对 WebSocket / protobuf 写入 `system.visible_to_others` 之类的保留键最直接。
func MetadataBoolBytes(value bool) []byte {
	if value {
		return []byte("true")
	}
	return []byte("false")
}

func (v *MetadataTypedValue) validate() error {
	if v == nil {
		return fmt.Errorf("typed_value is required")
	}
	switch v.Kind {
	case MetadataTypedValueKindBytes:
		if v.BytesValue == nil {
			return fmt.Errorf("typed_value.bytes_value is required")
		}
	case MetadataTypedValueKindBool:
		if v.BoolValue == nil {
			return fmt.Errorf("typed_value.bool_value is required")
		}
	case MetadataTypedValueKindString:
		if v.StringValue == nil {
			return fmt.Errorf("typed_value.string_value is required")
		}
	case MetadataTypedValueKindNumber:
		if v.NumberValue == nil {
			return fmt.Errorf("typed_value.number_value is required")
		}
		if _, err := normalizeMetadataNumberJSON(*v.NumberValue); err != nil {
			return err
		}
	case MetadataTypedValueKindJSON:
		if v.JSONValue == nil {
			return fmt.Errorf("typed_value.json_value is required")
		}
		if _, err := compactMetadataJSON(*v.JSONValue); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported typed_value.kind %q", v.Kind)
	}
	return nil
}

// MarshalJSON 实现 metadata 请求体的 value / typed_value 二选一编码。
func (r UpsertUserMetadataRequest) MarshalJSON() ([]byte, error) {
	if err := r.validateJSON(); err != nil {
		return nil, err
	}

	type payload struct {
		Value      *[]byte             `json:"value,omitempty"`
		TypedValue *MetadataTypedValue `json:"typed_value,omitempty"`
		ExpiresAt  *string             `json:"expires_at,omitempty"`
	}

	out := payload{
		TypedValue: cloneMetadataTypedValue(r.TypedValue),
		ExpiresAt:  r.ExpiresAt,
	}
	if r.Value != nil {
		value := cloneBytesPreserveNil(r.Value)
		out.Value = &value
	}
	return json.Marshal(out)
}

// UnmarshalJSON 支持从 HTTP metadata 请求 JSON 恢复公开请求模型。
func (r *UpsertUserMetadataRequest) UnmarshalJSON(data []byte) error {
	type payload struct {
		Value      *[]byte             `json:"value,omitempty"`
		TypedValue *MetadataTypedValue `json:"typed_value,omitempty"`
		ExpiresAt  *string             `json:"expires_at,omitempty"`
	}

	var in payload
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}

	out := UpsertUserMetadataRequest{
		TypedValue: cloneMetadataTypedValue(in.TypedValue),
		ExpiresAt:  in.ExpiresAt,
	}
	if in.Value != nil {
		out.Value = cloneBytesPreserveNil(*in.Value)
	}
	if err := out.validateJSON(); err != nil {
		return err
	}
	*r = out
	return nil
}

func (r UpsertUserMetadataRequest) clone() UpsertUserMetadataRequest {
	out := UpsertUserMetadataRequest{
		TypedValue: cloneMetadataTypedValue(r.TypedValue),
		ExpiresAt:  r.ExpiresAt,
	}
	if r.Value != nil {
		out.Value = cloneBytesPreserveNil(r.Value)
	}
	return out
}

func (r UpsertUserMetadataRequest) validateJSON() error {
	hasValue := r.Value != nil
	hasTyped := r.TypedValue != nil
	if hasValue == hasTyped {
		return fmt.Errorf("exactly one of value or typed_value is required")
	}
	if r.TypedValue != nil {
		return r.TypedValue.validate()
	}
	return nil
}

func validateHTTPMetadataUpsertRequest(key string, req UpsertUserMetadataRequest) error {
	if err := req.validateJSON(); err != nil {
		return err
	}
	return validateMetadataWritePolicy(key, req, true)
}

func validateWSMetadataUpsertRequest(key string, req UpsertUserMetadataRequest) error {
	if req.TypedValue != nil {
		return fmt.Errorf("typed_value is only supported by HTTP metadata APIs")
	}
	if req.Value == nil {
		return fmt.Errorf("value is required")
	}
	return validateMetadataWritePolicy(key, req, false)
}

func validateMetadataWritePolicy(key string, req UpsertUserMetadataRequest, allowTyped bool) error {
	if err := validateMetadataKeyPolicy(key); err != nil {
		return err
	}
	if req.TypedValue != nil {
		if !allowTyped {
			return fmt.Errorf("typed_value is only supported by HTTP metadata APIs")
		}
		if err := req.TypedValue.validate(); err != nil {
			return err
		}
	}
	if key != UserMetadataKeyVisibleToOthers {
		return nil
	}
	if req.ExpiresAt != nil {
		return fmt.Errorf("metadata key %q does not allow expires_at", key)
	}
	if req.TypedValue != nil {
		if req.TypedValue.Kind != MetadataTypedValueKindBool {
			return fmt.Errorf("metadata key %q requires typed_value.kind %q", key, MetadataTypedValueKindBool)
		}
		return nil
	}
	if _, err := parseMetadataBoolBytes(req.Value); err != nil {
		return fmt.Errorf("metadata key %q requires raw bytes %q or %q", key, "true", "false")
	}
	return nil
}

func validateMetadataKeyPolicy(key string) error {
	if !strings.HasPrefix(key, UserMetadataSystemKeyPrefix) {
		return nil
	}
	if key == UserMetadataKeyVisibleToOthers {
		return nil
	}
	return fmt.Errorf("unsupported system metadata key %q", key)
}

func validateMetadataScanSystemPrefix(prefix, after string) error {
	for _, item := range []struct {
		value string
		name  string
	}{
		{value: prefix, name: "prefix"},
		{value: after, name: "after"},
	} {
		if !strings.HasPrefix(item.value, UserMetadataSystemKeyPrefix) {
			continue
		}
		if metadataSystemPrefixRegistered(item.value) {
			continue
		}
		return fmt.Errorf("unsupported %s %q", item.name, item.value)
	}
	return nil
}

func metadataSystemPrefixRegistered(prefix string) bool {
	return strings.HasPrefix(UserMetadataKeyVisibleToOthers, prefix) || strings.HasPrefix(prefix, UserMetadataKeyVisibleToOthers)
}

func parseMetadataBoolBytes(raw []byte) (bool, error) {
	switch strings.TrimSpace(string(raw)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("metadata value must be raw bytes %q or %q", "true", "false")
	}
}

func normalizeMetadataNumberJSON(raw json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("typed_value.number_value cannot be empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()

	var number json.Number
	if err := decoder.Decode(&number); err != nil {
		return nil, fmt.Errorf("typed_value.number_value must be a JSON number")
	}
	if err := ensureMetadataJSONEOF(decoder); err != nil {
		return nil, err
	}
	return json.RawMessage(number.String()), nil
}

func compactMetadataJSON(raw json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("typed_value.json_value cannot be empty")
	}
	if !json.Valid(trimmed) {
		return nil, fmt.Errorf("typed_value.json_value must be valid JSON")
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, trimmed); err != nil {
		return nil, fmt.Errorf("typed_value.json_value must be valid JSON")
	}
	return json.RawMessage(buf.Bytes()), nil
}

func ensureMetadataJSONEOF(decoder *json.Decoder) error {
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("typed JSON value must contain a single JSON value")
	}
	return nil
}

func cloneBytesPreserveNil(in []byte) []byte {
	if in == nil {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func cloneBytesOrEmpty(in []byte) []byte {
	if in == nil {
		return []byte{}
	}
	return cloneBytesPreserveNil(in)
}

func cloneRawMessagePreserveNil(in json.RawMessage) json.RawMessage {
	if in == nil {
		return nil
	}
	out := make(json.RawMessage, len(in))
	copy(out, in)
	return out
}

func cloneRawMessageOrEmpty(in json.RawMessage) json.RawMessage {
	if in == nil {
		return json.RawMessage{}
	}
	return cloneRawMessagePreserveNil(in)
}

func cloneMetadataTypedValue(in *MetadataTypedValue) *MetadataTypedValue {
	if in == nil {
		return nil
	}
	out := &MetadataTypedValue{
		Kind:        in.Kind,
		BoolValue:   in.BoolValue,
		StringValue: in.StringValue,
	}
	if in.BytesValue != nil {
		value := cloneBytesPreserveNil(*in.BytesValue)
		out.BytesValue = &value
	}
	if in.NumberValue != nil {
		value := cloneRawMessagePreserveNil(*in.NumberValue)
		out.NumberValue = &value
	}
	if in.JSONValue != nil {
		value := cloneRawMessagePreserveNil(*in.JSONValue)
		out.JSONValue = &value
	}
	return out
}
