package security

// password.go — password hashing for the auth config.
//
// Current scheme: PBKDF2-HMAC-SHA256 with a per-user random salt.
//
//	pbkdf2$sha256$<iterations>$<salt-b64>$<dk-b64>
//
// The legacy scheme (bare 64-char hex of SHA-256(password+username)) is still
// verified for backward compatibility so existing config files keep working,
// but LoadConfig logs a deprecation warning for every legacy entry.  Re-hash
// with:  veltrix-admin hash-password --user <u> --password <p>
//
// PBKDF2 is implemented here directly on top of crypto/hmac (RFC 8018 §5.2) —
// the project deliberately keeps its dependency set to the stdlib.

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// PBKDF2Iterations is the work factor for newly generated hashes.
// OWASP's 2023+ recommendation for PBKDF2-HMAC-SHA256 is 600k; 210k is the
// floor they list for interactive logins. We default high — hashing happens
// once per connection AUTH, not per request.
const PBKDF2Iterations = 210_000

const (
	pbkdf2Prefix  = "pbkdf2$sha256$"
	pbkdf2SaltLen = 16
	pbkdf2KeyLen  = 32
)

// pbkdf2Key derives a key per RFC 8018 §5.2 using HMAC-SHA256 as the PRF.
func pbkdf2Key(password, salt []byte, iter, keyLen int) []byte {
	prf := hmac.New(sha256.New, password)
	hashLen := prf.Size()
	numBlocks := (keyLen + hashLen - 1) / hashLen

	dk := make([]byte, 0, numBlocks*hashLen)
	var block [4]byte
	for i := 1; i <= numBlocks; i++ {
		prf.Reset()
		prf.Write(salt)
		binary.BigEndian.PutUint32(block[:], uint32(i))
		prf.Write(block[:])
		u := prf.Sum(nil)
		t := make([]byte, len(u))
		copy(t, u)
		for n := 2; n <= iter; n++ {
			prf.Reset()
			prf.Write(u)
			u = prf.Sum(u[:0])
			for x := range t {
				t[x] ^= u[x]
			}
		}
		dk = append(dk, t...)
	}
	return dk[:keyLen]
}

// HashPasswordPBKDF2 returns a self-describing PBKDF2 hash string with a
// fresh random salt, for use in the auth config file.
func HashPasswordPBKDF2(password string) (string, error) {
	salt := make([]byte, pbkdf2SaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate salt: %w", err)
	}
	dk := pbkdf2Key([]byte(password), salt, PBKDF2Iterations, pbkdf2KeyLen)
	return fmt.Sprintf("%s%d$%s$%s",
		pbkdf2Prefix, PBKDF2Iterations,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(dk)), nil
}

// isLegacyHash reports whether stored looks like the legacy bare
// SHA-256(password+username) hex digest.
func isLegacyHash(stored string) bool {
	if len(stored) != 64 {
		return false
	}
	for _, c := range stored {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// verifyPassword checks password against a stored hash in either scheme.
// Comparison is constant-time in both branches.
func verifyPassword(stored, password, username string) bool {
	if strings.HasPrefix(stored, pbkdf2Prefix) {
		rest := strings.TrimPrefix(stored, pbkdf2Prefix)
		parts := strings.Split(rest, "$")
		if len(parts) != 3 {
			return false
		}
		iter, err := strconv.Atoi(parts[0])
		if err != nil || iter < 1 || iter > 10_000_000 {
			return false
		}
		salt, err := base64.RawStdEncoding.DecodeString(parts[1])
		if err != nil {
			return false
		}
		want, err := base64.RawStdEncoding.DecodeString(parts[2])
		if err != nil || len(want) == 0 {
			return false
		}
		got := pbkdf2Key([]byte(password), salt, iter, len(want))
		return subtle.ConstantTimeCompare(got, want) == 1
	}
	if isLegacyHash(stored) {
		expected := hashPassword(password, username)
		return subtle.ConstantTimeCompare([]byte(strings.ToLower(stored)), []byte(expected)) == 1
	}
	return false
}
