package turntf

import (
	"encoding/json"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

// PasswordSource 表示密码的来源类型，用于标识密码是明文待哈希还是已经哈希处理。
type PasswordSource string

const (
	// PasswordSourcePlain 表示密码为明文，客户端会自动进行 bcrypt 哈希处理。
	PasswordSourcePlain PasswordSource = "plain"
	// PasswordSourceHashed 表示密码已经是 bcrypt 哈希后的字符串，客户端直接使用。
	PasswordSourceHashed PasswordSource = "hashed"
)

// PasswordInput 封装密码输入，支持明文密码（自动哈希）和预哈希密码两种模式。
// 创建方式：使用 PlainPassword 传入明文，或使用 HashedPassword 传入已哈希的密码字符串。
type PasswordInput struct {
	Source  PasswordSource `json:"-"`
	Encoded string         `json:"-"`
}

// PlainPassword 将明文密码进行 bcrypt 哈希后封装为 PasswordInput。
// plain 为明文密码字符串，不能为空。返回的 PasswordInput 的 Source 为 PasswordSourcePlain。
func PlainPassword(plain string) (PasswordInput, error) {
	encoded, err := HashPassword(plain)
	if err != nil {
		return PasswordInput{}, err
	}
	return PasswordInput{
		Source:  PasswordSourcePlain,
		Encoded: encoded,
	}, nil
}

// MustPlainPassword 是 PlainPassword 的便捷版本，在密码为空时触发 panic。
// 适用于密码已确认合法的场景（如测试或配置初始化）。
func MustPlainPassword(plain string) PasswordInput {
	password, err := PlainPassword(plain)
	if err != nil {
		panic(err)
	}
	return password
}

// HashedPassword 将已通过 bcrypt 哈希的密码字符串封装为 PasswordInput。
// hash 为预计算的 bcrypt 哈希值。返回的 PasswordInput 的 Source 为 PasswordSourceHashed。
func HashedPassword(hash string) PasswordInput {
	return PasswordInput{
		Source:  PasswordSourceHashed,
		Encoded: hash,
	}
}

// HashPassword 使用 bcrypt 算法对明文密码进行哈希处理。
// plain 为明文密码字符串，不能为空。返回 bcrypt 哈希后的字符串。
func HashPassword(plain string) (string, error) {
	if plain == "" {
		return "", fmt.Errorf("password is required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// Validate 校验 PasswordInput 是否合法：Source 必须为有效的来源类型且密码内容不能为空。
func (p PasswordInput) Validate() error {
	if p.Source != PasswordSourcePlain && p.Source != PasswordSourceHashed {
		return fmt.Errorf("invalid password source %q", p.Source)
	}
	if p.Encoded == "" {
		return fmt.Errorf("password is required")
	}
	return nil
}

// WireValue 返回密码在网络上传输的原始值（即编码后的字符串）。
func (p PasswordInput) WireValue() string {
	return p.Encoded
}

// IsHashed 判断 PasswordInput 是否包含有效的密码编码值（非空）。
func (p PasswordInput) IsHashed() bool {
	return p.Encoded != ""
}

// IsZero 判断 PasswordInput 是否为空（未设置任何值）。
func (p PasswordInput) IsZero() bool {
	return p.Source == "" && p.Encoded == ""
}

// MarshalJSON 将 PasswordInput 序列化为 JSON，输出编码后的密码字符串。
func (p PasswordInput) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Encoded)
}

// UnmarshalJSON 从 JSON 字符串反序列化 PasswordInput，默认将值解析为已哈希的密码。
func (p *PasswordInput) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*p = HashedPassword(value)
	return nil
}
