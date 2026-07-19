package storage

// backup_cloud.go — Cloud backup uploader (S3 / GCS / Azure Blob) for VeltrixDB.
//
// No external SDKs are used.  All cloud APIs are spoken via stdlib net/http
// with hand-rolled authentication:
//
//   S3    — AWS Signature Version 4 (SigV4).
//           Multipart upload automatically kicks in for files > multipartThreshold.
//   GCS   — Bearer token. If GCSCredFile is set, a JWT is signed with the
//           service-account private key and exchanged for an access token via
//           the OAuth2 token endpoint.
//   Azure — Shared Key (HMAC-SHA256) as documented in the Azure Storage REST API.
//
// Retry policy: up to 3 attempts on 5xx or network errors, with 1 s / 2 s / 4 s
// backoffs (capped at maxRetryDelay).
//
// All exported methods are safe for concurrent use.  Per-request state is
// heap-allocated inside each call; the CloudBackupUploader struct is immutable
// after construction.

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── Configuration ──────────────────────────────────────────────────────────────

// CloudProvider selects the backend.
type CloudProvider string

const (
	ProviderS3    CloudProvider = "s3"
	ProviderGCS   CloudProvider = "gcs"
	ProviderAzure CloudProvider = "azure"
)

// CloudBackupConfig holds all configuration for a cloud backup uploader.
// Fields are read at construction time; the struct is not mutated afterwards.
type CloudBackupConfig struct {
	Provider CloudProvider
	Bucket   string // S3 bucket or GCS bucket; Azure uses AzureContainer
	Prefix   string // key prefix / "folder" path inside the bucket

	// S3-specific (also checked from env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
	// AWS_REGION, AWS_SESSION_TOKEN)
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// GCS-specific
	GCSAccessToken string // raw Bearer token; mutually exclusive with GCSCredFile
	GCSCredFile    string // path to service-account JSON; JWT signed & exchanged

	// Azure-specific
	AzureAccount   string // AZURE_STORAGE_ACCOUNT
	AzureKey       string // AZURE_STORAGE_KEY (base64-encoded Shared Key)
	AzureContainer string // AZURE_STORAGE_CONTAINER
}

// fillFromEnv overlays environment variables into the config for fields that
// the caller left blank.
func (c *CloudBackupConfig) fillFromEnv() {
	if c.AccessKeyID == "" {
		c.AccessKeyID = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if c.SecretAccessKey == "" {
		c.SecretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	if c.Region == "" {
		c.Region = os.Getenv("AWS_REGION")
	}
	if c.SessionToken == "" {
		c.SessionToken = os.Getenv("AWS_SESSION_TOKEN")
	}
	if c.GCSAccessToken == "" {
		c.GCSAccessToken = os.Getenv("GCS_ACCESS_TOKEN")
	}
	if c.GCSCredFile == "" {
		c.GCSCredFile = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	}
	if c.AzureAccount == "" {
		c.AzureAccount = os.Getenv("AZURE_STORAGE_ACCOUNT")
	}
	if c.AzureKey == "" {
		c.AzureKey = os.Getenv("AZURE_STORAGE_KEY")
	}
	if c.AzureContainer == "" {
		c.AzureContainer = os.Getenv("AZURE_STORAGE_CONTAINER")
	}
	if c.Bucket == "" && c.Provider == ProviderAzure {
		c.Bucket = c.AzureContainer
	}
}

// ── CloudBackupUploader ────────────────────────────────────────────────────────

const (
	multipartThreshold = 100 << 20 // 100 MB — use S3 multipart above this
	multipartPartSize  = 64 << 20  // 64 MB per part
	maxRetryAttempts   = 3
	maxRetryDelay      = 4 * time.Second
)

// CloudBackupEntry describes one backup stored in cloud storage.
type CloudBackupEntry struct {
	CloudPath string
	BackupID  string
	Timestamp int64
	SizeBytes int64
}

// CloudBackupUploader uploads and downloads backup directories to/from cloud
// object storage.  Construct with NewCloudBackupUploader; all methods are safe
// for concurrent use.
type CloudBackupUploader struct {
	cfg    CloudBackupConfig
	client *http.Client

	// GCS token cache — refreshed lazily, guarded by mu.
	mu       sync.Mutex
	gcsToken string
	gcsExp   time.Time
}

// NewCloudBackupUploader creates an uploader from cfg.
// Environment variables are overlaid for any fields left blank in cfg.
func NewCloudBackupUploader(cfg CloudBackupConfig) *CloudBackupUploader {
	cfg.fillFromEnv()
	return &CloudBackupUploader{
		cfg:    cfg,
		client: &http.Client{Timeout: 5 * time.Minute},
	}
}

// Upload walks all files under backupDir and uploads them to cloud storage
// under <prefix>/<backupID>/.  It returns the cloud path prefix
// (e.g. "backups/full-1234567890/") which can be passed to Download later.
func (u *CloudBackupUploader) Upload(backupDir string) (string, error) {
	// Derive a stable backupID from the manifest if present; otherwise from dir name.
	backupID := filepath.Base(backupDir)
	if m, err := ReadManifest(backupDir); err == nil {
		backupID = m.BackupID
	}

	cloudRoot := strings.TrimRight(u.cfg.Prefix, "/")
	if cloudRoot != "" {
		cloudRoot += "/"
	}
	cloudRoot += backupID + "/"

	var totalBytes int64
	var fileCount int

	err := filepath.Walk(backupDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		rel, err := filepath.Rel(backupDir, path)
		if err != nil {
			return err
		}
		// Always use forward slashes as cloud key separators.
		objectKey := cloudRoot + filepath.ToSlash(rel)

		size := info.Size()
		log.Printf("[cloud-backup] uploading %s → %s (%d bytes)", rel, objectKey, size)

		if err := u.uploadFile(path, objectKey, size); err != nil {
			return fmt.Errorf("upload %s: %w", rel, err)
		}
		totalBytes += size
		fileCount++
		return nil
	})
	if err != nil {
		return "", err
	}

	log.Printf("[cloud-backup] upload complete  backup=%s  files=%d  bytes=%d",
		backupID, fileCount, totalBytes)
	return cloudRoot, nil
}

// Download fetches all objects under cloudPath from cloud storage and writes
// them to destDir, recreating the directory structure.
func (u *CloudBackupUploader) Download(cloudPath, destDir string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("mkdir %s: %w", destDir, err)
	}

	keys, err := u.listObjects(cloudPath)
	if err != nil {
		return fmt.Errorf("list %s: %w", cloudPath, err)
	}
	if len(keys) == 0 {
		return fmt.Errorf("no objects found under %s", cloudPath)
	}

	for _, key := range keys {
		// Strip the cloudPath prefix to get the relative path.
		rel := strings.TrimPrefix(key, cloudPath)
		rel = filepath.FromSlash(rel)
		dest := filepath.Join(destDir, rel)

		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return fmt.Errorf("mkdir for %s: %w", dest, err)
		}
		log.Printf("[cloud-backup] downloading %s → %s", key, dest)
		if err := u.downloadFile(key, dest); err != nil {
			return fmt.Errorf("download %s: %w", key, err)
		}
	}
	return nil
}

// ListBackups lists all backup manifests stored under the configured prefix.
func (u *CloudBackupUploader) ListBackups() ([]CloudBackupEntry, error) {
	prefix := strings.TrimRight(u.cfg.Prefix, "/") + "/"
	// List top-level "directories" (common prefixes) under prefix.
	prefixes, err := u.listCommonPrefixes(prefix)
	if err != nil {
		return nil, err
	}

	var entries []CloudBackupEntry
	for _, p := range prefixes {
		manifestKey := p + "manifest.json"
		data, err := u.getObjectBytes(manifestKey)
		if err != nil {
			// Missing manifest — skip; might be a partial upload.
			continue
		}
		var m BackupManifest
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		// Compute total size from disk metadata.
		var size int64
		for _, d := range m.Disks {
			size += d.VLogEndOff - d.VLogStartOff
		}
		entries = append(entries, CloudBackupEntry{
			CloudPath: p,
			BackupID:  m.BackupID,
			Timestamp: m.Timestamp,
			SizeBytes: size,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Timestamp < entries[j].Timestamp
	})
	return entries, nil
}

// ── Per-provider dispatch ───────────────────────────────────────────────────────

func (u *CloudBackupUploader) uploadFile(localPath, objectKey string, size int64) error {
	switch u.cfg.Provider {
	case ProviderS3:
		if size >= multipartThreshold {
			return u.s3MultipartUpload(localPath, objectKey)
		}
		data, err := os.ReadFile(localPath)
		if err != nil {
			return err
		}
		return u.s3PutObject(objectKey, data)
	case ProviderGCS:
		data, err := os.ReadFile(localPath)
		if err != nil {
			return err
		}
		return u.gcsPutObject(objectKey, data)
	case ProviderAzure:
		data, err := os.ReadFile(localPath)
		if err != nil {
			return err
		}
		return u.azurePutBlob(objectKey, data)
	default:
		return fmt.Errorf("unknown cloud provider: %q", u.cfg.Provider)
	}
}

func (u *CloudBackupUploader) downloadFile(objectKey, localPath string) error {
	data, err := u.getObjectBytes(objectKey)
	if err != nil {
		return err
	}
	tmp := localPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, localPath)
}

func (u *CloudBackupUploader) getObjectBytes(objectKey string) ([]byte, error) {
	switch u.cfg.Provider {
	case ProviderS3:
		return u.s3GetObject(objectKey)
	case ProviderGCS:
		return u.gcsGetObject(objectKey)
	case ProviderAzure:
		return u.azureGetBlob(objectKey)
	default:
		return nil, fmt.Errorf("unknown cloud provider: %q", u.cfg.Provider)
	}
}

func (u *CloudBackupUploader) listObjects(prefix string) ([]string, error) {
	switch u.cfg.Provider {
	case ProviderS3:
		return u.s3ListObjects(prefix)
	case ProviderGCS:
		return u.gcsListObjects(prefix)
	case ProviderAzure:
		return u.azureListBlobs(prefix)
	default:
		return nil, fmt.Errorf("unknown cloud provider: %q", u.cfg.Provider)
	}
}

func (u *CloudBackupUploader) listCommonPrefixes(prefix string) ([]string, error) {
	switch u.cfg.Provider {
	case ProviderS3:
		return u.s3ListCommonPrefixes(prefix)
	case ProviderGCS:
		return u.gcsListCommonPrefixes(prefix)
	case ProviderAzure:
		// Azure doesn't have a native "common prefix" list; approximate it.
		return u.azureListCommonPrefixes(prefix)
	default:
		return nil, fmt.Errorf("unknown cloud provider: %q", u.cfg.Provider)
	}
}

// ── Retry helper ───────────────────────────────────────────────────────────────

// doWithRetry executes fn with exponential backoff on transient errors.
func (u *CloudBackupUploader) doWithRetry(fn func() (*http.Response, error)) (*http.Response, error) {
	var lastErr error
	delay := time.Second
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			log.Printf("[cloud-backup] retry attempt %d after %s: %v", attempt+1, delay, lastErr)
			time.Sleep(delay)
			if delay < maxRetryDelay {
				delay *= 2
			}
		}
		resp, err := fn()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode >= 500 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("all %d attempts failed: %w", maxRetryAttempts, lastErr)
}

// readResponseBody drains the response body and closes it, returning the bytes.
func readResponseBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// ═══════════════════════════════════════════════════════════════════════════════
// AWS S3 — SigV4
// ═══════════════════════════════════════════════════════════════════════════════

func s3Endpoint(bucket, region, key string) string {
	return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s",
		bucket, region, url.PathEscape(key))
}

// signS3Request signs req with AWS Signature Version 4.
// payload is the request body (used for hash; may be nil for GET/DELETE).
func signS3Request(req *http.Request, cfg CloudBackupConfig, payload []byte) error {
	now := time.Now().UTC()
	dateISO := now.Format("20060102T150405Z")
	dateShort := now.Format("20060102")
	service := "s3"

	// Payload hash.
	payloadHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA256("")
	if len(payload) > 0 {
		h := sha256.Sum256(payload)
		payloadHash = hex.EncodeToString(h[:])
	}

	req.Header.Set("x-amz-date", dateISO)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	if cfg.SessionToken != "" {
		req.Header.Set("x-amz-security-token", cfg.SessionToken)
	}

	// ── Step 1: Canonical request ──────────────────────────────────────────
	// Canonical URI: path-encoded.
	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Canonical query string: sorted by key.
	queryParts := []string{}
	for k, vs := range req.URL.Query() {
		for _, v := range vs {
			queryParts = append(queryParts,
				url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}
	sort.Strings(queryParts)
	canonicalQuery := strings.Join(queryParts, "&")

	// Canonical + signed headers.
	signedHeaderNames := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if cfg.SessionToken != "" {
		signedHeaderNames = append(signedHeaderNames, "x-amz-security-token")
	}
	sort.Strings(signedHeaderNames)

	var canonicalHeaders strings.Builder
	for _, h := range signedHeaderNames {
		val := req.Header.Get(h)
		if h == "host" {
			val = req.Host
			if val == "" {
				val = req.URL.Host
			}
		}
		canonicalHeaders.WriteString(h + ":" + strings.TrimSpace(val) + "\n")
	}
	signedHeaders := strings.Join(signedHeaderNames, ";")

	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")

	// ── Step 2: String to sign ─────────────────────────────────────────────
	credScope := dateShort + "/" + cfg.Region + "/" + service + "/aws4_request"
	crHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + dateISO + "\n" + credScope + "\n" +
		hex.EncodeToString(crHash[:])

	// ── Step 3: Signing key ────────────────────────────────────────────────
	signingKey := s3DeriveKey(cfg.SecretAccessKey, dateShort, cfg.Region, service)

	// ── Step 4: Signature ──────────────────────────────────────────────────
	mac := hmac.New(sha256.New, signingKey)
	mac.Write([]byte(stringToSign))
	signature := hex.EncodeToString(mac.Sum(nil))

	auth := "AWS4-HMAC-SHA256 " +
		"Credential=" + cfg.AccessKeyID + "/" + credScope + ", " +
		"SignedHeaders=" + signedHeaders + ", " +
		"Signature=" + signature
	req.Header.Set("Authorization", auth)
	return nil
}

func s3DeriveKey(secret, date, region, service string) []byte {
	h := func(key []byte, data string) []byte {
		mac := hmac.New(sha256.New, key)
		mac.Write([]byte(data))
		return mac.Sum(nil)
	}
	kDate := h([]byte("AWS4"+secret), date)
	kRegion := h(kDate, region)
	kService := h(kRegion, service)
	return h(kService, "aws4_request")
}

// s3PutObject uploads value to key in the configured S3 bucket.
func (u *CloudBackupUploader) s3PutObject(key string, value []byte) error {
	endpoint := s3Endpoint(u.cfg.Bucket, u.cfg.Region, key)
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(value))
		if err != nil {
			return nil, err
		}
		req.ContentLength = int64(len(value))
		req.Header.Set("Content-Type", "application/octet-stream")
		if err := signS3Request(req, u.cfg, value); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent &&
			resp.StatusCode != http.StatusCreated {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("S3 PUT %s: HTTP %d: %s", key, resp.StatusCode, body)
		}
		_, _ = readResponseBody(resp)
		return resp, nil
	})
	return err
}

// s3GetObject downloads key from the configured S3 bucket.
func (u *CloudBackupUploader) s3GetObject(key string) ([]byte, error) {
	endpoint := s3Endpoint(u.cfg.Bucket, u.cfg.Region, key)
	var result []byte
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		if err := signS3Request(req, u.cfg, nil); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, fmt.Errorf("S3 GET %s: not found", key)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("S3 GET %s: HTTP %d: %s", key, resp.StatusCode, body)
		}
		data, err := readResponseBody(resp)
		if err != nil {
			return nil, err
		}
		result = data
		return resp, nil
	})
	return result, err
}

// s3DeleteObject deletes key from the configured S3 bucket.
func (u *CloudBackupUploader) s3DeleteObject(key string) error {
	endpoint := s3Endpoint(u.cfg.Bucket, u.cfg.Region, key)
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodDelete, endpoint, nil)
		if err != nil {
			return nil, err
		}
		if err := signS3Request(req, u.cfg, nil); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("S3 DELETE %s: HTTP %d: %s", key, resp.StatusCode, body)
		}
		_, _ = readResponseBody(resp)
		return resp, nil
	})
	return err
}

// s3ListObjects lists all object keys under prefix (no delimiter, recursive).
func (u *CloudBackupUploader) s3ListObjects(prefix string) ([]string, error) {
	return u.s3List(prefix, "")
}

// s3ListCommonPrefixes lists "subdirectory" prefixes directly under prefix.
func (u *CloudBackupUploader) s3ListCommonPrefixes(prefix string) ([]string, error) {
	return u.s3List(prefix, "/")
}

// s3List issues ListObjectsV2 requests and collects either keys or common-prefixes.
func (u *CloudBackupUploader) s3List(prefix, delimiter string) ([]string, error) {
	baseURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/", u.cfg.Bucket, u.cfg.Region)
	var results []string
	continuationToken := ""

	for {
		q := url.Values{}
		q.Set("list-type", "2")
		q.Set("prefix", prefix)
		if delimiter != "" {
			q.Set("delimiter", delimiter)
		}
		if continuationToken != "" {
			q.Set("continuation-token", continuationToken)
		}

		endpoint := baseURL + "?" + q.Encode()
		var body []byte
		_, err := u.doWithRetry(func() (*http.Response, error) {
			req, err := http.NewRequest(http.MethodGet, endpoint, nil)
			if err != nil {
				return nil, err
			}
			if err := signS3Request(req, u.cfg, nil); err != nil {
				return nil, err
			}
			resp, err := u.client.Do(req)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode != http.StatusOK {
				b, _ := readResponseBody(resp)
				return nil, fmt.Errorf("S3 LIST: HTTP %d: %s", resp.StatusCode, b)
			}
			data, err := readResponseBody(resp)
			if err != nil {
				return nil, err
			}
			body = data
			return resp, nil
		})
		if err != nil {
			return nil, err
		}

		// Minimal XML parse — avoid importing encoding/xml for just two tags.
		if delimiter != "" {
			results = append(results, xmlExtractAll(string(body), "Prefix")...)
		} else {
			results = append(results, xmlExtractAll(string(body), "Key")...)
		}

		// Check IsTruncated and NextContinuationToken.
		truncated := xmlExtractFirst(string(body), "IsTruncated")
		if strings.EqualFold(truncated, "false") || truncated == "" {
			break
		}
		continuationToken = xmlExtractFirst(string(body), "NextContinuationToken")
		if continuationToken == "" {
			break
		}
	}
	return results, nil
}

// ── S3 Multipart Upload ────────────────────────────────────────────────────────

// s3PartInfo describes a single completed multipart upload part.
type s3PartInfo struct {
	Number int
	ETag   string
}

// s3MultipartUpload uploads a large file using S3 multipart upload.
func (u *CloudBackupUploader) s3MultipartUpload(localPath, key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Pre-compute total part count for logging.
	var totalParts int
	if info, err := f.Stat(); err == nil && info.Size() > 0 {
		totalParts = int((info.Size() + multipartPartSize - 1) / multipartPartSize)
	}

	// 1. Initiate multipart upload.
	uploadID, err := u.s3InitiateMultipart(key)
	if err != nil {
		return fmt.Errorf("initiate multipart: %w", err)
	}

	var parts []s3PartInfo
	buf := make([]byte, multipartPartSize)
	partNumber := 1

	for {
		n, readErr := io.ReadFull(f, buf)
		if n == 0 {
			break
		}
		chunk := buf[:n]
		etag, err := u.s3UploadPart(key, uploadID, partNumber, chunk)
		if err != nil {
			// Abort so the bucket isn't left with dangling parts.
			_ = u.s3AbortMultipart(key, uploadID)
			return fmt.Errorf("upload part %d: %w", partNumber, err)
		}
		parts = append(parts, s3PartInfo{Number: partNumber, ETag: etag})
		log.Printf("[cloud-backup] s3 multipart key=%s part=%d/%d", key, partNumber, totalParts)
		partNumber++
		if readErr == io.ErrUnexpectedEOF || readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = u.s3AbortMultipart(key, uploadID)
			return readErr
		}
	}

	// 2. Complete.
	return u.s3CompleteMultipart(key, uploadID, parts)
}

func (u *CloudBackupUploader) s3InitiateMultipart(key string) (string, error) {
	endpoint := s3Endpoint(u.cfg.Bucket, u.cfg.Region, key) + "?uploads"
	var uploadID string
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPost, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		if err := signS3Request(req, u.cfg, nil); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		body, err := readResponseBody(resp)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("S3 initiate multipart: HTTP %d: %s", resp.StatusCode, body)
		}
		uploadID = xmlExtractFirst(string(body), "UploadId")
		if uploadID == "" {
			return nil, fmt.Errorf("S3 initiate multipart: no UploadId in response")
		}
		return resp, nil
	})
	return uploadID, err
}

func (u *CloudBackupUploader) s3UploadPart(key, uploadID string, partNum int, data []byte) (string, error) {
	endpoint := fmt.Sprintf("%s?partNumber=%d&uploadId=%s",
		s3Endpoint(u.cfg.Bucket, u.cfg.Region, key),
		partNum, url.QueryEscape(uploadID))
	var etag string
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		req.ContentLength = int64(len(data))
		req.Header.Set("Content-Type", "application/octet-stream")
		if err := signS3Request(req, u.cfg, data); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		body, err := readResponseBody(resp)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("S3 upload part %d: HTTP %d: %s", partNum, resp.StatusCode, body)
		}
		etag = strings.Trim(resp.Header.Get("ETag"), `"`)
		return resp, nil
	})
	return etag, err
}

func (u *CloudBackupUploader) s3CompleteMultipart(key, uploadID string, parts []s3PartInfo) error {
	// Build XML body.
	var sb strings.Builder
	sb.WriteString("<CompleteMultipartUpload>")
	for _, p := range parts {
		sb.WriteString(fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>\"%s\"</ETag></Part>",
			p.Number, p.ETag))
	}
	sb.WriteString("</CompleteMultipartUpload>")
	payload := []byte(sb.String())

	endpoint := s3Endpoint(u.cfg.Bucket, u.cfg.Region, key) +
		"?uploadId=" + url.QueryEscape(uploadID)
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.ContentLength = int64(len(payload))
		req.Header.Set("Content-Type", "application/xml")
		if err := signS3Request(req, u.cfg, payload); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		body, _ := readResponseBody(resp)
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("S3 complete multipart: HTTP %d: %s", resp.StatusCode, body)
		}
		return resp, nil
	})
	return err
}

func (u *CloudBackupUploader) s3AbortMultipart(key, uploadID string) error {
	endpoint := s3Endpoint(u.cfg.Bucket, u.cfg.Region, key) +
		"?uploadId=" + url.QueryEscape(uploadID)
	req, err := http.NewRequest(http.MethodDelete, endpoint, nil)
	if err != nil {
		return err
	}
	if err := signS3Request(req, u.cfg, nil); err != nil {
		return err
	}
	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	_, _ = readResponseBody(resp)
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// Google Cloud Storage — Bearer token / JWT
// ═══════════════════════════════════════════════════════════════════════════════

// getGCSToken returns a valid Bearer token, refreshing if expired.
func getGCSToken(cfg CloudBackupConfig) (string, error) {
	if cfg.GCSAccessToken != "" {
		return cfg.GCSAccessToken, nil
	}
	if cfg.GCSCredFile == "" {
		return "", fmt.Errorf("GCS: neither GCSAccessToken nor GCSCredFile is set")
	}
	return gcsTokenFromServiceAccount(cfg.GCSCredFile)
}

// gcsGetToken is the CloudBackupUploader method that honours the token cache.
func (u *CloudBackupUploader) gcsGetToken() (string, error) {
	// Fast path: raw token with no expiry to track.
	if u.cfg.GCSAccessToken != "" {
		return u.cfg.GCSAccessToken, nil
	}

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.gcsToken != "" && time.Now().Before(u.gcsExp) {
		return u.gcsToken, nil
	}
	tok, exp, err := gcsTokenFromServiceAccountWithExpiry(u.cfg.GCSCredFile)
	if err != nil {
		return "", err
	}
	u.gcsToken = tok
	u.gcsExp = exp
	return tok, nil
}

// serviceAccountJSON is the minimal shape of a GCP service-account key file.
type serviceAccountJSON struct {
	Type                    string `json:"type"`
	ProjectID               string `json:"project_id"`
	PrivateKeyID            string `json:"private_key_id"`
	PrivateKey              string `json:"private_key"`
	ClientEmail             string `json:"client_email"`
	TokenURI                string `json:"token_uri"`
}

func gcsTokenFromServiceAccount(credFile string) (string, error) {
	tok, _, err := gcsTokenFromServiceAccountWithExpiry(credFile)
	return tok, err
}

func gcsTokenFromServiceAccountWithExpiry(credFile string) (string, time.Time, error) {
	raw, err := os.ReadFile(credFile)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read GCS cred file: %w", err)
	}
	var sa serviceAccountJSON
	if err := json.Unmarshal(raw, &sa); err != nil {
		return "", time.Time{}, fmt.Errorf("parse GCS cred file: %w", err)
	}
	if sa.TokenURI == "" {
		sa.TokenURI = "https://oauth2.googleapis.com/token"
	}

	// Parse the PEM-encoded RSA private key.
	block, _ := pem.Decode([]byte(sa.PrivateKey))
	if block == nil {
		return "", time.Time{}, fmt.Errorf("GCS: no PEM block found in private_key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("GCS: parse PKCS8 key: %w", err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return "", time.Time{}, fmt.Errorf("GCS: private key is not RSA")
	}

	// Build JWT: header.payload.signature
	now := time.Now().Unix()
	exp := now + 3600
	header := base64.RawURLEncoding.EncodeToString(
		[]byte(`{"alg":"RS256","typ":"JWT"}`))
	claimsJSON, err := json.Marshal(map[string]interface{}{
		"iss":   sa.ClientEmail,
		"scope": "https://www.googleapis.com/auth/devstorage.read_write",
		"aud":   sa.TokenURI,
		"iat":   now,
		"exp":   exp,
	})
	if err != nil {
		return "", time.Time{}, err
	}
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)
	sigInput := header + "." + payload
	h := sha256.Sum256([]byte(sigInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, rsaKey, 0, h[:])
	if err != nil {
		return "", time.Time{}, fmt.Errorf("GCS JWT sign: %w", err)
	}
	jwt := sigInput + "." + base64.RawURLEncoding.EncodeToString(sig)

	// Exchange JWT for an access token.
	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", jwt)
	resp, err := http.PostForm(sa.TokenURI, form)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("GCS token exchange: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, fmt.Errorf("GCS token exchange: HTTP %d: %s", resp.StatusCode, body)
	}
	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", time.Time{}, fmt.Errorf("GCS token parse: %w", err)
	}
	expTime := time.Now().Add(time.Duration(tokenResp.ExpiresIn-60) * time.Second)
	return tokenResp.AccessToken, expTime, nil
}

func gcsObjectURL(bucket, key string) string {
	// URL-encode slashes inside the key for the REST API.
	return "https://storage.googleapis.com/storage/v1/b/" +
		url.PathEscape(bucket) + "/o/" + url.PathEscape(key)
}

func gcsUploadURL(bucket string) string {
	return "https://storage.googleapis.com/upload/storage/v1/b/" +
		url.PathEscape(bucket) + "/o?uploadType=media"
}

func (u *CloudBackupUploader) gcsPutObject(key string, value []byte) error {
	token, err := u.gcsGetToken()
	if err != nil {
		return err
	}
	uploadURL := gcsUploadURL(u.cfg.Bucket) + "&name=" + url.QueryEscape(key)
	_, err = u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPost, uploadURL, bytes.NewReader(value))
		if err != nil {
			return nil, err
		}
		req.ContentLength = int64(len(value))
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("GCS PUT %s: HTTP %d: %s", key, resp.StatusCode, body)
		}
		_, _ = readResponseBody(resp)
		return resp, nil
	})
	return err
}

func (u *CloudBackupUploader) gcsGetObject(key string) ([]byte, error) {
	token, err := u.gcsGetToken()
	if err != nil {
		return nil, err
	}
	// ?alt=media downloads the raw bytes.
	objectURL := gcsObjectURL(u.cfg.Bucket, key) + "?alt=media"
	var result []byte
	_, err = u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodGet, objectURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, fmt.Errorf("GCS GET %s: not found", key)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("GCS GET %s: HTTP %d: %s", key, resp.StatusCode, body)
		}
		data, err := readResponseBody(resp)
		if err != nil {
			return nil, err
		}
		result = data
		return resp, nil
	})
	return result, err
}

func (u *CloudBackupUploader) gcsDeleteObject(key string) error {
	token, err := u.gcsGetToken()
	if err != nil {
		return err
	}
	objectURL := gcsObjectURL(u.cfg.Bucket, key)
	_, err = u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodDelete, objectURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("GCS DELETE %s: HTTP %d: %s", key, resp.StatusCode, body)
		}
		_, _ = readResponseBody(resp)
		return resp, nil
	})
	return err
}

// gcsListObjects lists all objects with the given prefix (recursive, no delimiter).
func (u *CloudBackupUploader) gcsListObjects(prefix string) ([]string, error) {
	return u.gcsList(prefix, "")
}

// gcsListCommonPrefixes lists "subdirectory" prefixes directly under prefix.
func (u *CloudBackupUploader) gcsListCommonPrefixes(prefix string) ([]string, error) {
	return u.gcsList(prefix, "/")
}

func (u *CloudBackupUploader) gcsList(prefix, delimiter string) ([]string, error) {
	token, err := u.gcsGetToken()
	if err != nil {
		return nil, err
	}

	var results []string
	pageToken := ""

	for {
		params := url.Values{}
		params.Set("prefix", prefix)
		if delimiter != "" {
			params.Set("delimiter", delimiter)
		}
		if pageToken != "" {
			params.Set("pageToken", pageToken)
		}
		listURL := "https://storage.googleapis.com/storage/v1/b/" +
			url.PathEscape(u.cfg.Bucket) + "/o?" + params.Encode()

		var body []byte
		_, err = u.doWithRetry(func() (*http.Response, error) {
			req, err := http.NewRequest(http.MethodGet, listURL, nil)
			if err != nil {
				return nil, err
			}
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err := u.client.Do(req)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode != http.StatusOK {
				b, _ := readResponseBody(resp)
				return nil, fmt.Errorf("GCS LIST: HTTP %d: %s", resp.StatusCode, b)
			}
			data, err := readResponseBody(resp)
			if err != nil {
				return nil, err
			}
			body = data
			return resp, nil
		})
		if err != nil {
			return nil, err
		}

		// Parse JSON response.
		var resp struct {
			Items []struct {
				Name string `json:"name"`
			} `json:"items"`
			Prefixes      []string `json:"prefixes"`
			NextPageToken string   `json:"nextPageToken"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("GCS LIST parse: %w", err)
		}

		if delimiter != "" {
			results = append(results, resp.Prefixes...)
		} else {
			for _, item := range resp.Items {
				results = append(results, item.Name)
			}
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
	return results, nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// Azure Blob Storage — Shared Key
// ═══════════════════════════════════════════════════════════════════════════════

func azureBlobURL(account, container, blobName string) string {
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s",
		account, container, url.PathEscape(blobName))
}

func azureContainerURL(account, container string) string {
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s",
		account, container)
}

// signAzureRequest attaches an Authorization: SharedKey header to req.
// contentLength is the body length (0 for GET/DELETE).
// contentType is the Content-Type header value ("" for GET/DELETE).
func signAzureRequest(req *http.Request, cfg CloudBackupConfig, contentLength int64, contentType string) error {
	now := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Set("x-ms-date", now)
	req.Header.Set("x-ms-version", "2020-10-02")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	// ── Build string-to-sign ───────────────────────────────────────────────
	// https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
	//
	// StringToSign = VERB + "\n" +
	//   Content-Encoding + "\n" + Content-Language + "\n" +
	//   Content-Length + "\n" + Content-MD5 + "\n" +
	//   Content-Type + "\n" + Date + "\n" +
	//   If-Modified-Since + "\n" + If-Match + "\n" +
	//   If-None-Match + "\n" + If-Unmodified-Since + "\n" + Range + "\n" +
	//   CanonicalizedAmzHeaders + "\n" + CanonicalizedResource

	contentLenStr := ""
	if contentLength > 0 {
		contentLenStr = strconv.FormatInt(contentLength, 10)
	}

	// Canonicalized headers: all x-ms-* headers, sorted.
	var msHeaders []string
	for k := range req.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-ms-") {
			msHeaders = append(msHeaders, strings.ToLower(k))
		}
	}
	sort.Strings(msHeaders)
	var canonHdr strings.Builder
	for _, h := range msHeaders {
		canonHdr.WriteString(h + ":" + strings.TrimSpace(req.Header.Get(h)) + "\n")
	}

	// Canonicalized resource: /account/container/blob?sorted-params
	resource := "/" + cfg.AzureAccount + req.URL.Path
	qparams := req.URL.Query()
	if len(qparams) > 0 {
		var qparts []string
		for k, vs := range qparams {
			sort.Strings(vs)
			qparts = append(qparts, strings.ToLower(k)+":"+strings.Join(vs, ","))
		}
		sort.Strings(qparts)
		resource += "\n" + strings.Join(qparts, "\n")
	}

	stringToSign := strings.Join([]string{
		req.Method,
		"", // Content-Encoding
		"", // Content-Language
		contentLenStr,
		"", // Content-MD5
		contentType,
		"", // Date (superseded by x-ms-date)
		"", // If-Modified-Since
		"", // If-Match
		"", // If-None-Match
		"", // If-Unmodified-Since
		"", // Range
		strings.TrimRight(canonHdr.String(), "\n"),
		resource,
	}, "\n")

	// Decode the base64 account key and HMAC-SHA256.
	keyBytes, err := base64.StdEncoding.DecodeString(cfg.AzureKey)
	if err != nil {
		return fmt.Errorf("Azure: decode storage key: %w", err)
	}
	mac := hmac.New(sha256.New, keyBytes)
	mac.Write([]byte(stringToSign))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	req.Header.Set("Authorization",
		"SharedKey "+cfg.AzureAccount+":"+sig)
	return nil
}

func (u *CloudBackupUploader) azurePutBlob(blobName string, value []byte) error {
	blobURL := azureBlobURL(u.cfg.AzureAccount, u.cfg.AzureContainer, blobName)
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodPut, blobURL, bytes.NewReader(value))
		if err != nil {
			return nil, err
		}
		req.ContentLength = int64(len(value))
		req.Header.Set("x-ms-blob-type", "BlockBlob")
		if err := signAzureRequest(req, u.cfg, int64(len(value)), "application/octet-stream"); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("Azure PUT %s: HTTP %d: %s", blobName, resp.StatusCode, body)
		}
		_, _ = readResponseBody(resp)
		return resp, nil
	})
	return err
}

func (u *CloudBackupUploader) azureGetBlob(blobName string) ([]byte, error) {
	blobURL := azureBlobURL(u.cfg.AzureAccount, u.cfg.AzureContainer, blobName)
	var result []byte
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodGet, blobURL, nil)
		if err != nil {
			return nil, err
		}
		if err := signAzureRequest(req, u.cfg, 0, ""); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nil, fmt.Errorf("Azure GET %s: not found", blobName)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("Azure GET %s: HTTP %d: %s", blobName, resp.StatusCode, body)
		}
		data, err := readResponseBody(resp)
		if err != nil {
			return nil, err
		}
		result = data
		return resp, nil
	})
	return result, err
}

func (u *CloudBackupUploader) azureDeleteBlob(blobName string) error {
	blobURL := azureBlobURL(u.cfg.AzureAccount, u.cfg.AzureContainer, blobName)
	_, err := u.doWithRetry(func() (*http.Response, error) {
		req, err := http.NewRequest(http.MethodDelete, blobURL, nil)
		if err != nil {
			return nil, err
		}
		if err := signAzureRequest(req, u.cfg, 0, ""); err != nil {
			return nil, err
		}
		resp, err := u.client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
			body, _ := readResponseBody(resp)
			return nil, fmt.Errorf("Azure DELETE %s: HTTP %d: %s", blobName, resp.StatusCode, body)
		}
		_, _ = readResponseBody(resp)
		return resp, nil
	})
	return err
}

// azureListBlobs lists all blob names with the given prefix.
func (u *CloudBackupUploader) azureListBlobs(prefix string) ([]string, error) {
	return u.azureList(prefix, "")
}

// azureListCommonPrefixes lists "virtual directory" prefixes directly under prefix.
func (u *CloudBackupUploader) azureListCommonPrefixes(prefix string) ([]string, error) {
	return u.azureList(prefix, "/")
}

func (u *CloudBackupUploader) azureList(prefix, delimiter string) ([]string, error) {
	base := azureContainerURL(u.cfg.AzureAccount, u.cfg.AzureContainer)
	var results []string
	marker := ""

	for {
		q := url.Values{}
		q.Set("restype", "container")
		q.Set("comp", "list")
		q.Set("prefix", prefix)
		if delimiter != "" {
			q.Set("delimiter", delimiter)
		}
		if marker != "" {
			q.Set("marker", marker)
		}
		listURL := base + "?" + q.Encode()

		var body []byte
		_, err := u.doWithRetry(func() (*http.Response, error) {
			req, err := http.NewRequest(http.MethodGet, listURL, nil)
			if err != nil {
				return nil, err
			}
			if err := signAzureRequest(req, u.cfg, 0, ""); err != nil {
				return nil, err
			}
			resp, err := u.client.Do(req)
			if err != nil {
				return nil, err
			}
			if resp.StatusCode != http.StatusOK {
				b, _ := readResponseBody(resp)
				return nil, fmt.Errorf("Azure LIST: HTTP %d: %s", resp.StatusCode, b)
			}
			data, err := readResponseBody(resp)
			if err != nil {
				return nil, err
			}
			body = data
			return resp, nil
		})
		if err != nil {
			return nil, err
		}

		xml := string(body)
		if delimiter != "" {
			results = append(results, xmlExtractAll(xml, "BlobPrefix")...)
		} else {
			// Azure wraps each blob name in <Name>.
			results = append(results, xmlExtractAll(xml, "Name")...)
		}
		marker = xmlExtractFirst(xml, "NextMarker")
		if marker == "" {
			break
		}
	}
	return results, nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// ColdTier backends
// ═══════════════════════════════════════════════════════════════════════════════

// S3ColdTier implements ColdTier backed by AWS S3.
type S3ColdTier struct {
	u *CloudBackupUploader
}

// NewS3ColdTier constructs an S3ColdTier from cfg.
func NewS3ColdTier(cfg CloudBackupConfig) *S3ColdTier {
	cfg.Provider = ProviderS3
	return &S3ColdTier{u: NewCloudBackupUploader(cfg)}
}

func (t *S3ColdTier) Put(handle string, value []byte) error {
	return t.u.s3PutObject(t.key(handle), value)
}

func (t *S3ColdTier) Get(handle string) ([]byte, error) {
	return t.u.s3GetObject(t.key(handle))
}

func (t *S3ColdTier) Delete(handle string) error {
	return t.u.s3DeleteObject(t.key(handle))
}

func (t *S3ColdTier) Name() string {
	return fmt.Sprintf("s3://%s/%s", t.u.cfg.Bucket, t.u.cfg.Prefix)
}

func (t *S3ColdTier) key(handle string) string {
	if t.u.cfg.Prefix != "" {
		return strings.TrimRight(t.u.cfg.Prefix, "/") + "/" + handle
	}
	return handle
}

// GCSColdTier implements ColdTier backed by Google Cloud Storage.
type GCSColdTier struct {
	u *CloudBackupUploader
}

// NewGCSColdTier constructs a GCSColdTier from cfg.
func NewGCSColdTier(cfg CloudBackupConfig) *GCSColdTier {
	cfg.Provider = ProviderGCS
	return &GCSColdTier{u: NewCloudBackupUploader(cfg)}
}

func (t *GCSColdTier) Put(handle string, value []byte) error {
	return t.u.gcsPutObject(t.key(handle), value)
}

func (t *GCSColdTier) Get(handle string) ([]byte, error) {
	return t.u.gcsGetObject(t.key(handle))
}

func (t *GCSColdTier) Delete(handle string) error {
	return t.u.gcsDeleteObject(t.key(handle))
}

func (t *GCSColdTier) Name() string {
	return fmt.Sprintf("gcs://%s/%s", t.u.cfg.Bucket, t.u.cfg.Prefix)
}

func (t *GCSColdTier) key(handle string) string {
	if t.u.cfg.Prefix != "" {
		return strings.TrimRight(t.u.cfg.Prefix, "/") + "/" + handle
	}
	return handle
}

// AzureColdTier implements ColdTier backed by Azure Blob Storage.
type AzureColdTier struct {
	u *CloudBackupUploader
}

// NewAzureColdTier constructs an AzureColdTier from cfg.
func NewAzureColdTier(cfg CloudBackupConfig) *AzureColdTier {
	cfg.Provider = ProviderAzure
	return &AzureColdTier{u: NewCloudBackupUploader(cfg)}
}

func (t *AzureColdTier) Put(handle string, value []byte) error {
	return t.u.azurePutBlob(t.key(handle), value)
}

func (t *AzureColdTier) Get(handle string) ([]byte, error) {
	return t.u.azureGetBlob(t.key(handle))
}

func (t *AzureColdTier) Delete(handle string) error {
	return t.u.azureDeleteBlob(t.key(handle))
}

func (t *AzureColdTier) Name() string {
	return fmt.Sprintf("azure://%s/%s", t.u.cfg.AzureContainer, t.u.cfg.Prefix)
}

func (t *AzureColdTier) key(handle string) string {
	if t.u.cfg.Prefix != "" {
		return strings.TrimRight(t.u.cfg.Prefix, "/") + "/" + handle
	}
	return handle
}

// ═══════════════════════════════════════════════════════════════════════════════
// Minimal XML extraction helpers (no encoding/xml dependency)
// ═══════════════════════════════════════════════════════════════════════════════

// xmlExtractFirst returns the text content of the first occurrence of <tag>…</tag>.
func xmlExtractFirst(xml, tag string) string {
	open := "<" + tag + ">"
	close := "</" + tag + ">"
	start := strings.Index(xml, open)
	if start < 0 {
		return ""
	}
	start += len(open)
	end := strings.Index(xml[start:], close)
	if end < 0 {
		return ""
	}
	return xml[start : start+end]
}

// xmlExtractAll returns the text content of every <tag>…</tag> occurrence.
func xmlExtractAll(xml, tag string) []string {
	open := "<" + tag + ">"
	close := "</" + tag + ">"
	var results []string
	remaining := xml
	for {
		start := strings.Index(remaining, open)
		if start < 0 {
			break
		}
		start += len(open)
		end := strings.Index(remaining[start:], close)
		if end < 0 {
			break
		}
		results = append(results, remaining[start:start+end])
		remaining = remaining[start+end+len(close):]
	}
	return results
}

// ═══════════════════════════════════════════════════════════════════════════════
// Compile-time interface checks
// ═══════════════════════════════════════════════════════════════════════════════

// Ensure all three ColdTier implementations satisfy the interface at compile time.
var (
	_ ColdTier = (*S3ColdTier)(nil)
	_ ColdTier = (*GCSColdTier)(nil)
	_ ColdTier = (*AzureColdTier)(nil)
)

// ── unused-import guard ────────────────────────────────────────────────────────
// sha1 is imported for future HMAC-MD5 / SHA1 Content-MD5 use; silence the
// linter by referencing it explicitly.
var _ = sha1.New
