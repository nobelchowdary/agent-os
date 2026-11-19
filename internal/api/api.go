package api

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/nobelchowdary/agent-os/internal/backend"
	"github.com/nobelchowdary/agent-os/internal/config"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

type Server struct {
	Config     config.Config
	Principals []config.Principal
	Store      *store.Store
	Backend    backend.Backend
	Preview    bool
	Render     func(model.Record) (any, error)
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
	m.HandleFunc("GET /v1/templates", s.auth(func(w http.ResponseWriter, r *http.Request, tenant string) {
		write(w, 200, map[string]any{"templates": s.Config.Templates, "preview": s.Preview})
	}))
	m.HandleFunc("POST /v1/plan", s.auth(s.plan))
	m.HandleFunc("POST /v1/runs", s.auth(s.create))
	m.HandleFunc("GET /v1/runs", s.auth(s.list))
	m.HandleFunc("GET /v1/runs/{id}", s.auth(s.get))
	m.HandleFunc("POST /v1/runs/{id}/cancel", s.auth(s.cancel))
	m.HandleFunc("GET /v1/runs/{id}/logs", s.auth(s.logs))
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
		problem(w, 503, "storage unavailable")
		return
	}
	if err := s.Backend.Ready(r.Context()); err != nil {
		problem(w, 503, "sandbox prerequisites unavailable")
		return
	}
	write(w, 200, map[string]string{"status": "ready"})
}

type endpoint func(http.ResponseWriter, *http.Request, string)

func (s *Server) auth(next endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") || len(h) > 1024 {
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
	r.Body = http.MaxBytesReader(w, r.Body, 32*1024)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	var req model.CreateRequest
	if err := d.Decode(&req); err != nil {
		problem(w, 400, "invalid request JSON or unsupported field")
		return req, false
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		problem(w, 400, "expected one JSON object")
		return req, false
	}
	return req, true
}

func (s *Server) record(w http.ResponseWriter, r *http.Request, tenant string) (model.Record, bool) {
	id := r.PathValue("id")
	if !runID.MatchString(id) {
		problem(w, 404, "run not found")
		return model.Record{}, false
	}
	v, err := s.Store.Get(tenant, id)
	if errors.Is(err, store.ErrNotFound) {
		problem(w, 404, "run not found")
		return v, false
	}
	if err != nil {
		problem(w, 503, "storage unavailable")
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
		problem(w, 400, err.Error())
		return model.Record{}, false
	}
	id, err := NewID()
	if err != nil {
		problem(w, 500, "could not allocate run identity")
		return model.Record{}, false
	}
	now := time.Now().UTC()
	return model.Record{ID: id, TenantID: tenant, Spec: spec, State: model.Queued, CreatedAt: now, UpdatedAt: now}, true
}

func (s *Server) create(w http.ResponseWriter, r *http.Request, tenant string) {
	if s.Preview {
		problem(w, 503, "preview mode does not execute runs; use /v1/plan")
		return
	}
	v, ok := s.newRecord(w, r, tenant)
	if !ok {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 128 {
		problem(w, 400, "Idempotency-Key exceeds 128 bytes")
		return
	}
	for _, b := range []byte(key) {
		if b < 33 || b > 126 {
			problem(w, 400, "invalid Idempotency-Key")
			return
		}
	}
	v, created, err := s.Store.Admit(v, key, s.Config.MaxActivePerTenant, s.Config.MaxActiveTotal)
	switch {
	case errors.Is(err, store.ErrQuota):
		w.Header().Set("Retry-After", "5")
		problem(w, 429, "active run quota exceeded")
		return
	case errors.Is(err, store.ErrIdempotencyConflict):
		problem(w, 409, "Idempotency-Key already used with a different request")
		return
	case err != nil:
		problem(w, 503, "could not persist run")
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
		problem(w, 400, "environment cannot be rendered")
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
	v, err := s.Store.List(tenant, 100)
	if err != nil {
		problem(w, 503, "storage unavailable")
		return
	}
	if v == nil {
		v = []model.Record{}
	}
	write(w, 200, map[string]any{"runs": v})
}
func (s *Server) cancel(w http.ResponseWriter, r *http.Request, tenant string) {
	if s.Preview {
		problem(w, 503, "preview mode cannot cancel executions")
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
		problem(w, 409, "run changed; retry cancellation")
		return
	}
	if err != nil {
		problem(w, 503, "could not persist cancellation")
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
		problem(w, 503, "execution disabled")
		return
	}
	const limit int64 = 64 * 1024
	logs, err := s.Backend.Logs(r.Context(), v.ID, limit)
	if errors.Is(err, backend.ErrNotFound) {
		problem(w, 404, "execution logs unavailable")
		return
	}
	if err != nil {
		problem(w, 503, "execution logs unavailable")
		return
	}
	write(w, 200, map[string]any{"run_id": v.ID, "logs": logs, "limit_bytes": limit})
}

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func problem(w http.ResponseWriter, status int, message string) {
	write(w, status, map[string]string{"error": message})
}
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	problem(w, 401, "authentication required")
}
