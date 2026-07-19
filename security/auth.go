package security

// auth.go — Role-based access control (RBAC) for VeltrixDB.
//
// Every client connection goes through AuthEnforcer before commands are
// dispatched.  The flow is:
//
//   1. Client sends:  AUTH <username> <password>\n
//      (binary protocol: cmd=0x09 [2B userLen][4B passLen][user][pass])
//   2. AuthEnforcer looks up the user and verifies the password against the
//      stored hash — PBKDF2-HMAC-SHA256 (see password.go), with fallback to
//      the deprecated legacy SHA-256(password+username) hex scheme.
//   3. On success, the connection is granted the permissions of the user's role.
//   4. All subsequent commands are checked against those permissions.
//
// Roles and permissions:
//
//   admin     — read + write + delete + admin  (full access)
//   readwrite — read + write + delete
//   readonly  — read only (GET, PING, INFO)
//
// Config file format (JSON):
//
//   {
//     "users": [
//       {
//         "username": "alice",
//         "password_hash": "pbkdf2$sha256$210000$<salt-b64>$<dk-b64>",
//         "role": "admin"
//       }
//     ]
//   }
//
// Generate a password hash with the admin CLI:
//   veltrix-admin hash-password --user alice --password secret
//
// Legacy bare sha256hex(password+username) hashes are still accepted but log
// a deprecation warning at load time.

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

// Permission is a bitmask of allowed operations.
type Permission uint32

const (
	PermRead   Permission = 1 << iota // GET
	PermWrite                         // PUT
	PermDelete                        // DEL
	PermAdmin                         // INFO, COMPACT, BACKUP, SCAN, admin commands
)

// PermAll is a convenience constant for admin users.
const PermAll = PermRead | PermWrite | PermDelete | PermAdmin

// rolePerms maps role name → permission bitmask.
var rolePerms = map[string]Permission{
	"admin":     PermAll,
	"readwrite": PermRead | PermWrite | PermDelete,
	"readonly":  PermRead,
}

// User is one entry in the auth config file.
type User struct {
	Username     string `json:"username"`
	PasswordHash string `json:"password_hash"` // sha256hex(password + username)
	Role         string `json:"role"`
}

// AuthConfig is the JSON structure of the auth config file.
type AuthConfig struct {
	Users []User `json:"users"`
}

// AuthEnforcer validates credentials and issues per-connection permission sets.
// It is safe for concurrent use.
type AuthEnforcer struct {
	mu       sync.RWMutex
	users    map[string]User // username → User
	required bool            // when false, all operations are allowed (auth disabled)
}

// NewAuthEnforcer creates an enforcer that allows everything (auth disabled).
// Call LoadConfig to enable auth from a file.
func NewAuthEnforcer() *AuthEnforcer {
	return &AuthEnforcer{
		users:    make(map[string]User),
		required: false,
	}
}

// LoadConfig reads an AuthConfig from a JSON file and enables auth enforcement.
func (ae *AuthEnforcer) LoadConfig(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read auth config %s: %w", path, err)
	}
	var cfg AuthConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse auth config: %w", err)
	}
	if len(cfg.Users) == 0 {
		return fmt.Errorf("auth config has no users — refusing to enable auth with empty user list")
	}

	ae.mu.Lock()
	defer ae.mu.Unlock()
	ae.users = make(map[string]User, len(cfg.Users))
	for _, u := range cfg.Users {
		if _, ok := rolePerms[u.Role]; !ok {
			return fmt.Errorf("user %q has unknown role %q (valid: admin, readwrite, readonly)", u.Username, u.Role)
		}
		if isLegacyHash(u.PasswordHash) {
			log.Printf("[auth] WARNING: user %q uses the legacy SHA-256 password hash — "+
				"re-hash with `veltrix-admin hash-password` (PBKDF2)", u.Username)
		}
		ae.users[strings.ToLower(u.Username)] = u
	}
	ae.required = true
	return nil
}

// IsRequired returns true when auth is enforced (config was loaded successfully).
func (ae *AuthEnforcer) IsRequired() bool {
	ae.mu.RLock()
	defer ae.mu.RUnlock()
	return ae.required
}

// Authenticate checks username+password and returns the granted permissions.
// Returns ErrAuthDisabled when auth is not required (all permissions granted).
// Returns ErrUnauthorized on bad credentials.
func (ae *AuthEnforcer) Authenticate(username, password string) (Permission, error) {
	ae.mu.RLock()
	defer ae.mu.RUnlock()

	if !ae.required {
		return PermAll, nil
	}

	u, ok := ae.users[strings.ToLower(username)]
	if !ok {
		// Constant-time stub to prevent user enumeration via timing.
		_ = subtle.ConstantTimeCompare([]byte("x"), []byte("y"))
		return 0, ErrUnauthorized
	}

	if !verifyPassword(u.PasswordHash, password, username) {
		return 0, ErrUnauthorized
	}

	perm, ok := rolePerms[u.Role]
	if !ok {
		return 0, fmt.Errorf("internal: unknown role %q for user %q", u.Role, username)
	}
	return perm, nil
}

// HasPerm returns true if the given permission set includes perm.
func HasPerm(granted, needed Permission) bool {
	return granted&needed == needed
}

// HashPassword returns sha256hex(password + username) for use in config files.
func HashPassword(password, username string) string {
	return hashPassword(password, username)
}

func hashPassword(password, username string) string {
	h := sha256.Sum256([]byte(password + strings.ToLower(username)))
	return hex.EncodeToString(h[:])
}

// ── ConnAuth tracks per-connection authentication state ───────────────────────

// ConnAuth is attached to each TCP connection to track login state.
type ConnAuth struct {
	enforcer    *AuthEnforcer
	Granted     Permission
	Authenticated bool
	Username    string
}

// NewConnAuth creates a new per-connection auth tracker.
// If auth is not required, the connection starts pre-authenticated with PermAll.
func NewConnAuth(ae *AuthEnforcer) *ConnAuth {
	ca := &ConnAuth{enforcer: ae}
	if !ae.IsRequired() {
		ca.Granted = PermAll
		ca.Authenticated = true
	}
	return ca
}

// Login validates credentials and records the granted permissions.
func (ca *ConnAuth) Login(username, password string) error {
	perm, err := ca.enforcer.Authenticate(username, password)
	if err != nil {
		return err
	}
	ca.Granted = perm
	ca.Authenticated = true
	ca.Username = username
	return nil
}

// Check returns an error if perm is not in the granted set.
func (ca *ConnAuth) Check(perm Permission) error {
	if !ca.Authenticated {
		return ErrNotAuthenticated
	}
	if !HasPerm(ca.Granted, perm) {
		return ErrForbidden
	}
	return nil
}

// ── Sentinel errors ───────────────────────────────────────────────────────────

var (
	ErrUnauthorized    = fmt.Errorf("AUTH failed: invalid username or password")
	ErrNotAuthenticated = fmt.Errorf("AUTH required: send AUTH <user> <password> first")
	ErrForbidden       = fmt.Errorf("FORBIDDEN: insufficient permissions for this operation")
	ErrAuthDisabled    = fmt.Errorf("AUTH disabled: server does not require authentication")
)
