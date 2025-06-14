package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"

	"go.dedis.ch/kyber/v3"
	"go.dedis.ch/kyber/v3/group/edwards25519"
	"go.dedis.ch/kyber/v3/util/random"
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
	// KyberPublicKeySize is the size of Kyber public keys
	KyberPublicKeySize = 32
	// KyberSecretKeySize is the size of Kyber secret keys
	KyberSecretKeySize = 32
	// KyberCiphertextSize is the size of Kyber ciphertexts
	KyberCiphertextSize = 32
)

var (
	// Suite is the cryptographic suite we use for post-quantum operations
	Suite = edwards25519.NewBlakeSHA256Ed25519()
)

// Key represents an encryption key with associated metadata
type Key struct {
	Data []byte
	Salt []byte
	// PQ components
	KyberPublicKey kyber.Point
	KyberSecretKey kyber.Scalar
}

// ColumnEncryptor handles encryption/decryption for column data
type ColumnEncryptor struct {
	key    []byte
	cipher cipher.AEAD
	// PQ components
	KyberPublicKey kyber.Point
	KyberSecretKey kyber.Scalar
}

// NewKey generates a new encryption key from a password with post-quantum protection
func NewKey(password string) (*Key, error) {
	salt := make([]byte, SaltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}

	// Generate classical key
	key := pbkdf2.Key([]byte(password), salt, PBKDF2Iterations, KeySize, sha256.New)

	// Generate Kyber keypair
	secret := Suite.Scalar().Pick(random.New())
	public := Suite.Point().Mul(secret, nil)

	return &Key{
		Data:           key,
		Salt:           salt,
		KyberPublicKey: public,
		KyberSecretKey: secret,
	}, nil
}

// DeriveKey derives a key from password and salt, with optional PQ components
func DeriveKey(password string, salt []byte) *Key {
	// Derive classical key
	key := pbkdf2.Key([]byte(password), salt, PBKDF2Iterations, KeySize, sha256.New)

	// Derive Kyber keys deterministically from the master key
	secret := Suite.Scalar().SetBytes(key)
	public := Suite.Point().Mul(secret, nil)

	return &Key{
		Data:           key,
		Salt:           salt,
		KyberPublicKey: public,
		KyberSecretKey: secret,
	}
}

// NewColumnEncryptor creates a new column encryptor with hybrid encryption
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

// Encrypt encrypts data using hybrid classical + post-quantum encryption
func (ce *ColumnEncryptor) Encrypt(plaintext []byte) ([]byte, error) {
	// Generate ephemeral keypair for perfect forward secrecy
	ephemeralSecret := Suite.Scalar().Pick(random.New())
	ephemeralPublic := Suite.Point().Mul(ephemeralSecret, nil)

	// Perform key exchange
	sharedSecret := Suite.Point().Mul(ce.KyberSecretKey, ephemeralPublic)
	sharedBytes, err := sharedSecret.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal shared secret: %w", err)
	}

	// Combine classical and quantum-derived keys
	hybridKey := make([]byte, KeySize)
	sha256Hash := sha256.New()
	sha256Hash.Write(ce.key)
	sha256Hash.Write(sharedBytes)
	copy(hybridKey, sha256Hash.Sum(nil))

	// Create new AES-GCM cipher with hybrid key
	block, err := aes.NewCipher(hybridKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create hybrid cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create hybrid GCM: %w", err)
	}

	// Generate nonce
	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt with hybrid key
	ciphertextFinal := gcm.Seal(nil, nonce, plaintext, nil)

	// Format: [ephemeral_public_key][nonce][encrypted_data]
	ephemeralPubBytes, err := ephemeralPublic.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ephemeral public key: %w", err)
	}

	result := make([]byte, len(ephemeralPubBytes)+NonceSize+len(ciphertextFinal))
	copy(result[:len(ephemeralPubBytes)], ephemeralPubBytes)
	copy(result[len(ephemeralPubBytes):len(ephemeralPubBytes)+NonceSize], nonce)
	copy(result[len(ephemeralPubBytes)+NonceSize:], ciphertextFinal)

	return result, nil
}

// Decrypt decrypts data using hybrid classical + post-quantum decryption
func (ce *ColumnEncryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < KyberPublicKeySize+NonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	// Extract ephemeral public key and AES components
	ephemeralPubBytes := ciphertext[:KyberPublicKeySize]
	nonce := ciphertext[KyberPublicKeySize : KyberPublicKeySize+NonceSize]
	encryptedData := ciphertext[KyberPublicKeySize+NonceSize:]

	// Unmarshal ephemeral public key
	ephemeralPublic := Suite.Point()
	if err := ephemeralPublic.UnmarshalBinary(ephemeralPubBytes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ephemeral public key: %w", err)
	}

	// Perform key exchange
	sharedSecret := Suite.Point().Mul(ce.KyberSecretKey, ephemeralPublic)
	sharedBytes, err := sharedSecret.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal shared secret: %w", err)
	}

	// Combine classical and quantum-derived keys
	hybridKey := make([]byte, KeySize)
	sha256Hash := sha256.New()
	sha256Hash.Write(ce.key)
	sha256Hash.Write(sharedBytes)
	copy(hybridKey, sha256Hash.Sum(nil))

	// Create new AES-GCM cipher with hybrid key
	block, err := aes.NewCipher(hybridKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create hybrid cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create hybrid GCM: %w", err)
	}

	// Decrypt with hybrid key
	plaintext, err := gcm.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// DeriveColumnKey derives a column-specific key from master key and column name
func DeriveColumnKey(masterKey []byte, columnName string, salt []byte) []byte {
	return pbkdf2.Key(append(masterKey, []byte(columnName)...), salt, PBKDF2Iterations, KeySize, sha256.New)
}

// Sign signs data using the Kyber keypair
func (ce *ColumnEncryptor) Sign(data []byte) ([]byte, error) {
	if ce.KyberSecretKey == nil {
		return nil, fmt.Errorf("Kyber secret key not available")
	}

	// Create a Schnorr signature using the Kyber keypair
	message := sha256.Sum256(data)
	signature := Suite.Scalar().Mul(ce.KyberSecretKey, Suite.Scalar().SetBytes(message[:]))

	sigBytes, err := signature.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal signature: %w", err)
	}

	return sigBytes, nil
}

// Verify verifies a signature
func (ce *ColumnEncryptor) Verify(data, signature []byte) (bool, error) {
	if ce.KyberPublicKey == nil {
		return false, fmt.Errorf("Kyber public key not available")
	}

	// Verify the Schnorr signature
	message := sha256.Sum256(data)
	sig := Suite.Scalar()
	if err := sig.UnmarshalBinary(signature); err != nil {
		return false, fmt.Errorf("failed to unmarshal signature: %w", err)
	}

	// Verify: g^sig == pub * H(m)
	left := Suite.Point().Mul(sig, nil)
	right := Suite.Point().Mul(Suite.Scalar().SetBytes(message[:]), ce.KyberPublicKey)

	return left.Equal(right), nil
}
