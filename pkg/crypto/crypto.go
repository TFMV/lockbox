package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"golang.org/x/crypto/pbkdf2"
)

const (
	// KeySize is the size of AES-256 keys in bytes
	KeySize = 32
	// NonceSize is the size of AES-GCM nonces in bytes
	NonceSize = 12
	// SaltSize is the size of PBKDF2 salts in bytes
	SaltSize = 32
	// PBKDF2Iterations is the number of iterations for key derivation
	PBKDF2Iterations = 100000
)

// Key represents an encryption key with associated metadata
type Key struct {
	Data []byte
	Salt []byte
}

// ColumnEncryptor handles encryption/decryption for column data
type ColumnEncryptor struct {
	key    []byte
	cipher cipher.AEAD
}

// NewKey generates a new encryption key from a password
func NewKey(password string) (*Key, error) {
	salt := make([]byte, SaltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	key := pbkdf2.Key([]byte(password), salt, PBKDF2Iterations, KeySize, sha256.New)
	return &Key{
		Data: key,
		Salt: salt,
	}, nil
}

// DeriveKey derives a key from password and salt
func DeriveKey(password string, salt []byte) *Key {
	key := pbkdf2.Key([]byte(password), salt, PBKDF2Iterations, KeySize, sha256.New)
	return &Key{
		Data: key,
		Salt: salt,
	}
}

// NewColumnEncryptor creates a new column encryptor with the given key
func NewColumnEncryptor(key []byte) (*ColumnEncryptor, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("invalid key size: expected %d, got %d", KeySize, len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return &ColumnEncryptor{
		key:    key,
		cipher: gcm,
	}, nil
}

// Encrypt encrypts data using AES-256-GCM
func (ce *ColumnEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := ce.cipher.Seal(nil, nonce, plaintext, nil)

	// Prepend nonce to ciphertext
	result := make([]byte, NonceSize+len(ciphertext))
	copy(result[:NonceSize], nonce)
	copy(result[NonceSize:], ciphertext)

	return result, nil
}

// Decrypt decrypts data using AES-256-GCM
func (ce *ColumnEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < NonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce := ciphertext[:NonceSize]
	data := ciphertext[NonceSize:]

	plaintext, err := ce.cipher.Open(nil, nonce, data, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// DeriveColumnKey derives a column-specific key from master key and column name
func DeriveColumnKey(masterKey []byte, columnName string, salt []byte) []byte {
	return pbkdf2.Key(append(masterKey, []byte(columnName)...), salt, PBKDF2Iterations, KeySize, sha256.New)
}
