package security

import (
	"encoding/hex"
	"strings"
	"testing"
)

// RFC 6070-style test vector for PBKDF2-HMAC-SHA256 (from RFC 7914 §11 and
// the widely published SHA-256 vectors for RFC 6070 inputs).
func TestPBKDF2KnownVector(t *testing.T) {
	got := pbkdf2Key([]byte("password"), []byte("salt"), 4096, 32)
	want := "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134a"
	if hex.EncodeToString(got) != want {
		t.Fatalf("pbkdf2 vector mismatch:\n got %s\nwant %s", hex.EncodeToString(got), want)
	}
}

func TestPBKDF2KnownVectorMultiBlock(t *testing.T) {
	// keyLen > hash size exercises the multi-block path.
	got := pbkdf2Key([]byte("passwordPASSWORDpassword"), []byte("saltSALTsaltSALTsaltSALTsaltSALTsalt"), 4096, 40)
	want := "348c89dbcbd32b2f32d814b8116e84cf2b17347ebc1800181c4e2a1fb8dd53e1c635518c7dac47e9"
	if hex.EncodeToString(got) != want {
		t.Fatalf("pbkdf2 multiblock vector mismatch:\n got %s\nwant %s", hex.EncodeToString(got), want)
	}
}

func TestHashPasswordPBKDF2RoundTrip(t *testing.T) {
	h, err := HashPasswordPBKDF2("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(h, "pbkdf2$sha256$") {
		t.Fatalf("unexpected format: %s", h)
	}
	if !verifyPassword(h, "s3cret", "alice") {
		t.Fatal("correct password rejected")
	}
	if verifyPassword(h, "wrong", "alice") {
		t.Fatal("wrong password accepted")
	}
	// Two hashes of the same password must differ (random salt).
	h2, _ := HashPasswordPBKDF2("s3cret")
	if h == h2 {
		t.Fatal("salt not random: identical hashes")
	}
}

func TestVerifyLegacyHash(t *testing.T) {
	legacy := HashPassword("secret", "Alice")
	if !isLegacyHash(legacy) {
		t.Fatalf("legacy hash not recognized: %s", legacy)
	}
	if !verifyPassword(legacy, "secret", "Alice") {
		t.Fatal("legacy hash rejected correct password")
	}
	if verifyPassword(legacy, "nope", "Alice") {
		t.Fatal("legacy hash accepted wrong password")
	}
}

func TestVerifyPasswordMalformed(t *testing.T) {
	for _, bad := range []string{
		"",
		"pbkdf2$sha256$",
		"pbkdf2$sha256$abc$x$y",
		"pbkdf2$sha256$0$c2FsdA$c2FsdA",         // iter < 1
		"pbkdf2$sha256$99999999999$c2FsdA$c2FsdA", // absurd iter
		"pbkdf2$sha256$1000$!!!$c2FsdA",           // bad b64 salt
		"nothexnothexnothexnothexnothexnothexnothexnothexnothexnothexnot!", // 64 chars, not hex
	} {
		if verifyPassword(bad, "pw", "u") {
			t.Fatalf("malformed hash %q verified", bad)
		}
	}
}

func TestAuthenticatePBKDF2User(t *testing.T) {
	h, err := HashPasswordPBKDF2("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	ae := NewAuthEnforcer()
	ae.mu.Lock()
	ae.users["bob"] = User{Username: "bob", PasswordHash: h, Role: "readwrite"}
	ae.required = true
	ae.mu.Unlock()

	perm, err := ae.Authenticate("Bob", "hunter2")
	if err != nil {
		t.Fatalf("auth failed: %v", err)
	}
	if !HasPerm(perm, PermRead|PermWrite) {
		t.Fatal("readwrite perms missing")
	}
	if _, err := ae.Authenticate("Bob", "wrong"); err != ErrUnauthorized {
		t.Fatalf("want ErrUnauthorized, got %v", err)
	}
}
