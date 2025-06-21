package crypto

import (
	"fmt"
	"plugin"
)

// Encryptor defines encryption operations used by the rest of the system.
type Encryptor interface {
	Encrypt([]byte) ([]byte, error)
	Decrypt([]byte) ([]byte, error)
	Sign([]byte) ([]byte, error)
	Verify([]byte, []byte) (bool, error)
}

// Module provides cryptographic primitives. Different modules can
// implement alternative algorithms such as homomorphic encryption.
type Module interface {
	Name() string
	NewKey(password string) (*Key, error)
	DeriveKey(password string, salt []byte) *Key
	NewEncryptor(key []byte) (Encryptor, error)
}

var registry = map[string]Module{}

// RegisterModule registers a cryptographic module.
func RegisterModule(m Module) {
	if m != nil {
		registry[m.Name()] = m
	}
}

// GetModule retrieves a registered module by name.
func GetModule(name string) (Module, bool) {
	m, ok := registry[name]
	return m, ok
}

// LoadPlugin dynamically loads a module from a Go plugin.
// The plugin must expose a symbol named "Module" that implements Module.
func LoadPlugin(path string) error {
	p, err := plugin.Open(path)
	if err != nil {
		return err
	}
	sym, err := p.Lookup("Module")
	if err != nil {
		return err
	}
	mod, ok := sym.(Module)
	if !ok {
		return fmt.Errorf("invalid module type")
	}
	RegisterModule(mod)
	return nil
}

// defaultModule implements the existing crypto operations.
type defaultModule struct{}

func (defaultModule) Name() string                                { return "default" }
func (defaultModule) NewKey(password string) (*Key, error)        { return NewKey(password) }
func (defaultModule) DeriveKey(password string, salt []byte) *Key { return DeriveKey(password, salt) }
func (defaultModule) NewEncryptor(key []byte) (Encryptor, error) {
	return NewColumnEncryptor(key)
}

func init() {
	RegisterModule(defaultModule{})
}

var _ Encryptor = (*ColumnEncryptor)(nil)
