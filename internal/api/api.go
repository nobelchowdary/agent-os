package api

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/config"
	"github.com/nobelchowdary/agent-os/internal/controller"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

type Server struct {
	Config           config.Config
	Principals       []config.Principal
	Store            *store.Store
	Backend          backend.Backend
	Preview          bool
	Render           func(model.Record) (any, error)
	ControllerStatus func() controller.Status
	streamMu         sync.Mutex
	streamCounts     map[string]int
	streams          int
}

var runID = regexp.MustCompile(`^[a-f0-9]{32}$`)

func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s *Server) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	m.HandleFunc("GET /readyz", s.ready)
	m.HandleFunc("GET /v1/openapi.json", s.auth(func(w http.ResponseWriter, r *http.Request, tenant string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(openAPISpec)
	}))
	m.HandleFunc("GET /v1/templates", s.auth(func(w http.ResponseWriter, r *http.Request, tenant string) {
		write(w, 200, map[string]any{"templates": s.Config.Templates, "preview": s.Preview})
	}))
	m.HandleFunc("GET /v1/status", s.auth(s.status))
	m.HandleFunc("POST /v1/plan", s.auth(s.plan))
	m.HandleFunc("POST /v1/runs", s.auth(s.create))
	m.HandleFunc("GET /v1/runs", s.auth(s.list))
	m.HandleFunc("GET /v1/runs/{id}", s.auth(s.get))
	m.HandleFunc("POST /v1/runs/{id}/cancel", s.auth(s.cancel))
	m.HandleFunc("GET /v1/runs/{id}/logs", s.auth(s.logs))
	m.HandleFunc("GET /v1/runs/{id}/events", s.auth(s.events))
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		allowed := []string{}
		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
			probe := r.Clone(r.Context())
			probe.Method = method
			if _, pattern := m.Handler(probe); pattern != "" && pattern != "/" {
				allowed = append(allowed, method)
			}
		}
		if len(allowed) != 0 {
			w.Header().Set("Allow", strings.Join(allowed, ", "))
			problem(w, 405, "method_not_allowed", "method not allowed")
			return
		}
		problem(w, 404, "route_not_found", "route not found")
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		m.ServeHTTP(w, r)
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.Preview {
		write(w, 200, map[string]string{"status": "preview", "execution": "disabled"})
		return
	}
	if _, err := s.Store.Active(); err != nil {
		problem(w, 503, "storage_unavailable", "storage unavailable")
		return
	}
	if !s.controllerHealthy() {
		problem(w, 503, "controller_unavailable", "execution controller unavailable")
		return
	}
	if err := s.Backend.Ready(r.Context()); err != nil {
		problem(w, 503, "sandbox_unavailable", "sandbox prerequisites unavailable")
		return
	}
	write(w, 200, map[string]string{"status": "ready"})
}

type endpoint func(http.ResponseWriter, *http.Request, string)

func (s *Server) auth(next endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if len(r.Header.Values("Authorization")) != 1 || !strings.HasPrefix(h, "Bearer ") || len(h) > 1024 {
			unauthorized(w)
			return
		}
		token := strings.TrimPrefix(h, "Bearer ")
		sum := sha256.Sum256([]byte(token))
		tenant := ""
		for _, p := range s.Principals {
			if subtle.ConstantTimeCompare(sum[:], p.TokenHash[:]) == 1 {
				tenant = p.TenantID
			}
		}
		if tenant == "" {
			unauthorized(w)
			return
		}
		next(w, r, tenant)
	}
}

func (s *Server) readRequest(w http.ResponseWriter, r *http.Request) (model.CreateRequest, bool) {
	if contentType := r.Header.Get("Content-Type"); contentType != "" {
		mediaType, _, err := mime.ParseMediaType(contentType)
		if err != nil || mediaType != "application/json" {
			problem(w, 415, "unsupported_media_type", "expected application/json")
			return model.CreateRequest{}, false
		}
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32*1024)
	var req model.CreateRequest
	invalid := func() (model.CreateRequest, bool) {
		problem(w, 400, "invalid_request", "invalid request JSON or unsupported field")
		return req, false
	}
	body, err := io.ReadAll(r.Body)
	if err != nil || !utf8.Valid(body) {
		return invalid()
	}
	d := json.NewDecoder(bytes.NewReader(body))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return invalid()
	}
	seen := make(map[string]bool)
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok || seen[key] {
			return invalid()
		}
		seen[key] = true
		var raw json.RawMessage
		if err := d.Decode(&raw); err != nil || string(raw) == "null" {
			return invalid()
		}
		switch key {
		case "template":
			err = json.Unmarshal(raw, &req.Template)
		case "command":
			var arguments []json.RawMessage
			err = json.Unmarshal(raw, &arguments)
			if err != nil || len(arguments) == 0 || len(arguments) > 64 {
				return invalid()
			}
			req.Command = make([]string, len(arguments))
			for i, argument := range arguments {
				if string(argument) == "null" || json.Unmarshal(argument, &req.Command[i]) != nil {
					return invalid()
				}
			}
		case "timeout_seconds":
			err = json.Unmarshal(raw, &req.TimeoutSeconds)
			if req.TimeoutSeconds < 1 {
				return invalid()
			}
		default:
			return invalid()
		}
		if err != nil {
			return invalid()
		}
	}
	last, err := d.Token()
	if err != nil || last != json.Delim('}') || !seen["template"] {
		return invalid()
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		problem(w, 400, "invalid_request", "expected one JSON object")
		return req, false
	}
	return req, true
}

func (s *Server) record(w http.ResponseWriter, r *http.Request, tenant string) (model.Record, bool) {
	id := r.PathValue("id")
	if !runID.MatchString(id) {
		problem(w, 404, "run_not_found", "run not found")
		return model.Record{}, false
	}
	v, err := s.Store.Get(tenant, id)
	if errors.Is(err, store.ErrNotFound) {
		problem(w, 404, "run_not_found", "run not found")
		return v, false
	}
	if err != nil {
		problem(w, 503, "storage_unavailable", "storage unavailable")
		return v, false
	}
	return v, true
}

func (s *Server) newRecord(w http.ResponseWriter, r *http.Request, tenant string) (model.Record, bool) {
	req, ok := s.readRequest(w, r)
	if !ok {
		return model.Record{}, false
	}
	spec, err := s.Config.Resolve(req)
	if err != nil {
		problem(w, 400, "invalid_request", err.Error())
		return model.Record{}, false
	}
	id, err := NewID()
	if err != nil {
		problem(w, 500, "internal_error", "could not allocate run identity")
		return model.Record{}, false
	}
	now := time.Now().UTC()
	return model.Record{ID: id, TenantID: tenant, Spec: spec, State: model.Queued, CreatedAt: now, UpdatedAt: now}, true
}

func (s *Server) create(w http.ResponseWriter, r *http.Request, tenant string) {
	if s.Preview {
		problem(w, 503, "execution_disabled", "preview mode does not execute runs; use /v1/plan")
		return
	}
	v, ok := s.newRecord(w, r, tenant)
	if !ok {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(r.Header.Values("Idempotency-Key")) > 1 || (len(r.Header.Values("Idempotency-Key")) == 1 && key == "") {
		problem(w, 400, "invalid_idempotency_key", "expected one nonempty Idempotency-Key")
		return
	}
	if len(key) > 128 {
		problem(w, 400, "invalid_idempotency_key", "Idempotency-Key exceeds 128 bytes")
		return
	}
	for _, b := range []byte(key) {
		if b < 33 || b > 126 {
			problem(w, 400, "invalid_idempotency_key", "invalid Idempotency-Key")
			return
		}
	}
	v, created, err := s.Store.Admit(v, key, s.Config.MaxActivePerTenant, s.Config.MaxActiveTotal)
	switch {
	case errors.Is(err, store.ErrQuota):
		w.Header().Set("Retry-After", "5")
		problem(w, 429, "quota_exceeded", "active run quota exceeded")
		return
	case errors.Is(err, store.ErrIdempotencyConflict):
		problem(w, 409, "idempotency_conflict", "Idempotency-Key already used with a different request")
		return
	case err != nil:
		problem(w, 503, "storage_unavailable", "could not persist run")
		return
	}
	w.Header().Set("Location", "/v1/runs/"+v.ID)
	status := http.StatusAccepted
	if !created {
		status = http.StatusOK
	}
	write(w, status, v)
}

func (s *Server) plan(w http.ResponseWriter, r *http.Request, tenant string) {
	v, ok := s.newRecord(w, r, tenant)
	if !ok {
		return
	}
	job, err := s.Render(v)
	if err != nil {
		problem(w, 400, "invalid_environment", "environment cannot be rendered")
		return
	}
	write(w, 200, map[string]any{"mode": "preview", "execution": false, "job": job})
}

func (s *Server) get(w http.ResponseWriter, r *http.Request, tenant string) {
	if v, ok := s.record(w, r, tenant); ok {
		write(w, 200, v)
	}
}
func (s *Server) list(w http.ResponseWriter, r *http.Request, tenant string) {
	query, err := strictQuery(r, "limit", "before")
	if err != nil {
		problem(w, 400, "invalid_pagination", "invalid pagination parameters")
		return
	}
	limit := 100
	if raw, ok := query["limit"]; ok {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			problem(w, 400, "invalid_pagination", "limit must be between 1 and 100")
			return
		}
		limit = n
	}
	v, next, err := s.Store.ListPage(tenant, limit, query["before"])
	if errors.Is(err, store.ErrCursor) {
		problem(w, 400, "invalid_cursor", "invalid pagination cursor")
		return
	}
	if err != nil {
		problem(w, 503, "storage_unavailable", "storage unavailable")
		return
	}
	if v == nil {
		v = []model.Record{}
	}
	write(w, 200, map[string]any{"runs": v, "next_cursor": next})
}
func (s *Server) cancel(w http.ResponseWriter, r *http.Request, tenant string) {
	if s.Preview {
		problem(w, 503, "execution_disabled", "preview mode cannot cancel executions")
		return
	}
	v, ok := s.record(w, r, tenant)
	if !ok {
		return
	}
	if v.State.Terminal() || v.State == model.Cancelling {
		write(w, 200, v)
		return
	}
	v, err := s.Store.Transition(v.ID, []model.State{v.State}, model.Cancelling, "cancel requested")
	if errors.Is(err, store.ErrConflict) {
		problem(w, 409, "run_conflict", "run changed; retry cancellation")
		return
	}
	if err != nil {
		problem(w, 503, "storage_unavailable", "could not persist cancellation")
		return
	}
	write(w, 202, v)
}
func (s *Server) logs(w http.ResponseWriter, r *http.Request, tenant string) {
	v, ok := s.record(w, r, tenant)
	if !ok {
		return
	}
	if s.Preview {
		problem(w, 503, "execution_disabled", "execution disabled")
		return
	}
	const limit int64 = 64 * 1024
	logs, err := s.Backend.Logs(r.Context(), v.ID, limit)
	if errors.Is(err, backend.ErrNotFound) {
		problem(w, 404, "logs_unavailable", "execution logs unavailable")
		return
	}
	if err != nil {
		problem(w, 503, "logs_unavailable", "execution logs unavailable")
		return
	}
	// JSON replaces invalid UTF-8, which can expand raw log bytes. Apply the
	// public byte bound to the valid text actually returned to clients too.
	logs = strings.ToValidUTF8(logs, "\uFFFD")
	if len(logs) > int(limit) {
		logs = logs[:limit]
		for !utf8.ValidString(logs) {
			logs = logs[:len(logs)-1]
		}
	}
	write(w, 200, map[string]any{"run_id": v.ID, "logs": logs, "limit_bytes": limit})
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, code, message string) {
	if code == "storage_unavailable" || code == "controller_unavailable" || code == "sandbox_unavailable" || code == "logs_unavailable" {
		w.Header().Set("Retry-After", "5")
	}
	write(w, status, map[string]string{"error": message, "code": code})
}
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	problem(w, 401, "unauthenticated", "authentication required")
}
