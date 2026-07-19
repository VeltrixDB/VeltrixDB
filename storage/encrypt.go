package storage

// encrypt.go — at-rest encryption for VLog values.
//
// Algorithm: AES-256-GCM. Each record carries its own 12-byte nonce so the
// same key can encrypt billions of records without nonce reuse (NIST allows
// up to 2^32 random nonces under one key with negligible collision risk).
//
// Key management:
//   - The key is loaded once at engine startup from VELTRIXDB_ENCRYPTION_KEY
//     (32-byte base64-encoded value) or from a file path passed via
//     StorageConfig.EncryptionKeyPath.  We deliberately do NOT log or expose
//     the key via metrics/admin API.
//   - The same key must be present on every restart; otherwise reads of
//     previously-encrypted records will fail with "decryption: cipher: ...".
//   - Key rotation is out of scope for this implementation — operators rotate
//     by writing a new key, scrubbing through all records (read+rewrite),
//     and then retiring the old key. Future work: per-record key-version byte.
//
// Encrypted record layout (in addition to FlagEncrypted on the IndexEntry):
//   value bytes on disk  =  [12B nonce][N+16B AES-GCM ciphertext+tag]
// IndexEntry.UncompressedSize stays the original plaintext length so the
// reader knows how big a buffer to allocate.
//
// Interaction with FlagCompressed: encryption is applied AFTER compression on
// the write path and BEFORE decompression on the read path.  This is the
// correct ordering — encrypted ciphertext is incompressible, so compress
// first, encrypt second.

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

// EncryptionKeyEnvVar is the environment variable name searched first for the
// encryption key. The value must be 32 raw bytes encoded as standard base64.
const EncryptionKeyEnvVar = "VELTRIXDB_ENCRYPTION_KEY"

// encryptor holds an AEAD cipher built once at startup.  Methods are safe for
// concurrent use because crypto/cipher.AEAD is documented as concurrent-safe.
type encryptor struct {
	aead cipher.AEAD
}

// globalEncryptor is set when StorageConfig.EncryptionEnabled is true and a
// valid key is loaded.  Hot-path Encrypt/Decrypt look it up via getEncryptor()
// so the engine code paths can remain backing-agnostic.
var (
	globalEncryptor   *encryptor
	globalEncryptorMu sync.RWMutex
)

func setEncryptor(e *encryptor) {
	globalEncryptorMu.Lock()
	globalEncryptor = e
	globalEncryptorMu.Unlock()
}

func getEncryptor() *encryptor {
	globalEncryptorMu.RLock()
	defer globalEncryptorMu.RUnlock()
	return globalEncryptor
}

// loadEncryptor reads a 32-byte AES key from VELTRIXDB_ENCRYPTION_KEY
// (base64) or from a key file at keyPath. Returns nil, nil when both are empty
// (encryption explicitly disabled). Errors only when a key was specified but
// could not be decoded or was the wrong length.
func loadEncryptor(keyPath string) (*encryptor, error) {
	var raw []byte
	if v := strings.TrimSpace(os.Getenv(EncryptionKeyEnvVar)); v != "" {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("encryption: %s is not valid base64: %w", EncryptionKeyEnvVar, err)
		}
		raw = b
	} else if keyPath != "" {
		b, err := os.ReadFile(keyPath)
		if err != nil {
			return nil, fmt.Errorf("encryption: read key file %s: %w", keyPath, err)
		}
		raw = b
		// File may have trailing newline; strip whitespace.
		raw = []byte(strings.TrimSpace(string(raw)))
		// If still base64-shaped, decode.
		if dec, err := base64.StdEncoding.DecodeString(string(raw)); err == nil && len(dec) == 32 {
			raw = dec
		}
	} else {
		return nil, nil // encryption disabled
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("encryption: key must be exactly 32 bytes (got %d)", len(raw))
	}
	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, fmt.Errorf("encryption: aes init: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: gcm init: %w", err)
	}
	return &encryptor{aead: aead}, nil
}

// Encrypt seals plaintext under the global key and returns
// [12B nonce][ciphertext+tag].  Returns plaintext unchanged when encryption is
// disabled — callers should NOT set FlagEncrypted in that case.
func Encrypt(plaintext []byte) ([]byte, bool, error) {
	e := getEncryptor()
	if e == nil {
		return plaintext, false, nil
	}
	nonce := make([]byte, e.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, false, fmt.Errorf("encrypt: nonce: %w", err)
	}
	out := make([]byte, 0, len(nonce)+len(plaintext)+e.aead.Overhead())
	out = append(out, nonce...)
	out = e.aead.Seal(out, nonce, plaintext, nil)
	return out, true, nil
}

// Decrypt opens [12B nonce][ciphertext+tag]. Errors on AEAD verification fail.
func Decrypt(blob []byte) ([]byte, error) {
	e := getEncryptor()
	if e == nil {
		return nil, errors.New("decrypt: encryption is disabled but record has FlagEncrypted")
	}
	nonceSize := e.aead.NonceSize()
	if len(blob) < nonceSize+e.aead.Overhead() {
		return nil, fmt.Errorf("decrypt: blob too short (%d bytes)", len(blob))
	}
	nonce := blob[:nonceSize]
	ct := blob[nonceSize:]
	pt, err := e.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return pt, nil
}

// EncryptionEnabled reports whether a key has been loaded.
func EncryptionEnabled() bool {
	return getEncryptor() != nil
}
