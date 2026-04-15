package turntf

import (
	"encoding/json"
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

type PasswordSource string

const (
	PasswordSourcePlain  PasswordSource = "plain"
	PasswordSourceHashed PasswordSource = "hashed"
)

type PasswordInput struct {
	Source  PasswordSource `json:"-"`
	Encoded string         `json:"-"`
}

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

func MustPlainPassword(plain string) PasswordInput {
	password, err := PlainPassword(plain)
	if err != nil {
		panic(err)
	}
	return password
}

func HashedPassword(hash string) PasswordInput {
	return PasswordInput{
		Source:  PasswordSourceHashed,
		Encoded: hash,
	}
}

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

func (p PasswordInput) Validate() error {
	if p.Source != PasswordSourcePlain && p.Source != PasswordSourceHashed {
		return fmt.Errorf("invalid password source %q", p.Source)
	}
	if p.Encoded == "" {
		return fmt.Errorf("password is required")
	}
	return nil
}

func (p PasswordInput) WireValue() string {
	return p.Encoded
}

func (p PasswordInput) IsHashed() bool {
	return p.Encoded != ""
}

func (p PasswordInput) IsZero() bool {
	return p.Source == "" && p.Encoded == ""
}

func (p PasswordInput) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Encoded)
}

func (p *PasswordInput) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*p = HashedPassword(value)
	return nil
}
