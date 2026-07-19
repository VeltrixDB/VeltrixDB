package security

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// ── HashPassword ──────────────────────────────────────────────────────────────

func TestHashPassword_Deterministic(t *testing.T) {
	h1 := HashPassword("secret", "alice")
	h2 := HashPassword("secret", "alice")
	if h1 != h2 {
		t.Fatal("HashPassword is not deterministic")
	}
}

func TestHashPassword_DifferentInputs(t *testing.T) {
	h1 := HashPassword("secret", "alice")
	h2 := HashPassword("secret", "bob")
	h3 := HashPassword("other", "alice")
	if h1 == h2 || h1 == h3 {
		t.Fatal("HashPassword must produce different hashes for different inputs")
	}
}

func TestHashPassword_CaseInsensitiveUsername(t *testing.T) {
	h1 := HashPassword("secret", "Alice")
	h2 := HashPassword("secret", "alice")
	if h1 != h2 {
		t.Fatal("HashPassword should normalise username to lowercase")
	}
}

// ── LoadConfig ────────────────────────────────────────────────────────────────

func writeAuthConfig(t *testing.T, users []User) string {
	t.Helper()
	cfg := AuthConfig{Users: users}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	f := filepath.Join(t.TempDir(), "auth.json")
	if err := os.WriteFile(f, data, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return f
}

func TestLoadConfig_Valid(t *testing.T) {
	ae := NewAuthEnforcer()
	if ae.IsRequired() {
		t.Fatal("auth should not be required before LoadConfig")
	}

	path := writeAuthConfig(t, []User{
		{Username: "alice", PasswordHash: HashPassword("pass", "alice"), Role: "admin"},
	})
	if err := ae.LoadConfig(path); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !ae.IsRequired() {
		t.Fatal("auth should be required after LoadConfig")
	}
}

func TestLoadConfig_EmptyUsers_Error(t *testing.T) {
	ae := NewAuthEnforcer()
	path := writeAuthConfig(t, []User{})
	if err := ae.LoadConfig(path); err == nil {
		t.Fatal("expected error for empty user list")
	}
}

func TestLoadConfig_UnknownRole_Error(t *testing.T) {
	ae := NewAuthEnforcer()
	path := writeAuthConfig(t, []User{
		{Username: "eve", PasswordHash: "x", Role: "superuser"},
	})
	if err := ae.LoadConfig(path); err == nil {
		t.Fatal("expected error for unknown role")
	}
}

func TestLoadConfig_FileNotFound(t *testing.T) {
	ae := NewAuthEnforcer()
	if err := ae.LoadConfig("/nonexistent/path/auth.json"); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadConfig_InvalidJSON(t *testing.T) {
	f := filepath.Join(t.TempDir(), "auth.json")
	_ = os.WriteFile(f, []byte("not json"), 0600)
	ae := NewAuthEnforcer()
	if err := ae.LoadConfig(f); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// ── Authenticate ──────────────────────────────────────────────────────────────

func TestAuthenticate_Disabled_ReturnsPermAll(t *testing.T) {
	ae := NewAuthEnforcer()
	perm, err := ae.Authenticate("anyone", "anything")
	if err != nil {
		t.Fatalf("expected no error when auth disabled, got %v", err)
	}
	if perm != PermAll {
		t.Fatalf("expected PermAll when disabled, got %v", perm)
	}
}

func TestAuthenticate_Admin_Success(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "admin", PasswordHash: HashPassword("adminpass", "admin"), Role: "admin"},
	})
	perm, err := ae.Authenticate("admin", "adminpass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if perm != PermAll {
		t.Fatalf("admin should have PermAll, got %v", perm)
	}
}

func TestAuthenticate_ReadOnly_Success(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "reader", PasswordHash: HashPassword("rpass", "reader"), Role: "readonly"},
	})
	perm, err := ae.Authenticate("reader", "rpass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if perm != PermRead {
		t.Fatalf("readonly should have PermRead only, got %v", perm)
	}
}

func TestAuthenticate_ReadWrite_Success(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "rw", PasswordHash: HashPassword("rwpass", "rw"), Role: "readwrite"},
	})
	perm, err := ae.Authenticate("rw", "rwpass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := PermRead | PermWrite | PermDelete
	if perm != expected {
		t.Fatalf("readwrite should have %v, got %v", expected, perm)
	}
}

func TestAuthenticate_WrongPassword(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "alice", PasswordHash: HashPassword("correct", "alice"), Role: "admin"},
	})
	_, err := ae.Authenticate("alice", "wrong")
	if err != ErrUnauthorized {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func TestAuthenticate_UnknownUser(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "alice", PasswordHash: HashPassword("pass", "alice"), Role: "admin"},
	})
	_, err := ae.Authenticate("bob", "anything")
	if err != ErrUnauthorized {
		t.Fatalf("expected ErrUnauthorized, got %v", err)
	}
}

func TestAuthenticate_CaseInsensitiveUsername(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "Alice", PasswordHash: HashPassword("pass", "Alice"), Role: "readonly"},
	})
	// authenticate with lowercase — should succeed because both sides normalise
	perm, err := ae.Authenticate("alice", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if perm != PermRead {
		t.Fatalf("expected PermRead, got %v", perm)
	}
}

// ── HasPerm ───────────────────────────────────────────────────────────────────

func TestHasPerm_Included(t *testing.T) {
	if !HasPerm(PermAll, PermRead) {
		t.Fatal("PermAll must include PermRead")
	}
	if !HasPerm(PermAll, PermWrite) {
		t.Fatal("PermAll must include PermWrite")
	}
	if !HasPerm(PermAll, PermAdmin) {
		t.Fatal("PermAll must include PermAdmin")
	}
}

func TestHasPerm_Excluded(t *testing.T) {
	if HasPerm(PermRead, PermWrite) {
		t.Fatal("PermRead must not grant PermWrite")
	}
	if HasPerm(0, PermRead) {
		t.Fatal("zero permissions must not grant anything")
	}
}

// ── ConnAuth ──────────────────────────────────────────────────────────────────

func TestConnAuth_AuthDisabled_PreAuthenticated(t *testing.T) {
	ae := NewAuthEnforcer()
	ca := NewConnAuth(ae)
	if !ca.Authenticated {
		t.Fatal("ConnAuth should start authenticated when auth is disabled")
	}
	if ca.Granted != PermAll {
		t.Fatal("ConnAuth should have PermAll when auth is disabled")
	}
}

func TestConnAuth_Login_Success(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "u", PasswordHash: HashPassword("p", "u"), Role: "admin"},
	})
	ca := NewConnAuth(ae)
	if ca.Authenticated {
		t.Fatal("ConnAuth should not start authenticated when auth is required")
	}
	if err := ca.Login("u", "p"); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if !ca.Authenticated {
		t.Fatal("ConnAuth should be authenticated after successful Login")
	}
	if ca.Username != "u" {
		t.Fatalf("username not set: got %q", ca.Username)
	}
}

func TestConnAuth_Login_BadPassword(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "u", PasswordHash: HashPassword("right", "u"), Role: "readonly"},
	})
	ca := NewConnAuth(ae)
	if err := ca.Login("u", "wrong"); err == nil {
		t.Fatal("expected error on bad password")
	}
}

func TestConnAuth_Check_BeforeLogin(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "u", PasswordHash: HashPassword("p", "u"), Role: "admin"},
	})
	ca := NewConnAuth(ae)
	if err := ca.Check(PermRead); err != ErrNotAuthenticated {
		t.Fatalf("expected ErrNotAuthenticated before login, got %v", err)
	}
}

func TestConnAuth_Check_InsufficientPerm(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "r", PasswordHash: HashPassword("p", "r"), Role: "readonly"},
	})
	ca := NewConnAuth(ae)
	_ = ca.Login("r", "p")
	if err := ca.Check(PermWrite); err != ErrForbidden {
		t.Fatalf("expected ErrForbidden for readonly user writing, got %v", err)
	}
}

func TestConnAuth_Check_SufficientPerm(t *testing.T) {
	ae := loadedEnforcer(t, []User{
		{Username: "a", PasswordHash: HashPassword("p", "a"), Role: "admin"},
	})
	ca := NewConnAuth(ae)
	_ = ca.Login("a", "p")
	if err := ca.Check(PermAdmin); err != nil {
		t.Fatalf("unexpected error for admin user: %v", err)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func loadedEnforcer(t *testing.T, users []User) *AuthEnforcer {
	t.Helper()
	ae := NewAuthEnforcer()
	path := writeAuthConfig(t, users)
	if err := ae.LoadConfig(path); err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	return ae
}
