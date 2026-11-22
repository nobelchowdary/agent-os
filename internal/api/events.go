package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nobelchowdary/agent-os/internal/controller"
	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

const (
	maxStreams         = 32
	maxTenantStreams   = 4
	streamLifetime     = 60 * time.Second
	streamHeartbeat    = 10 * time.Second
	streamWriteTimeout = 5 * time.Second
	maxReplayEvents    = 256
)

func strictQuery(r *http.Request, allowed ...string) (map[string]string, error) {
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for key, entries := range values {
		known := false
		for _, name := range allowed {
			known = known || key == name
		}
		if !known || len(entries) != 1 {
			return nil, errors.New("invalid query")
		}
		result[key] = entries[0]
	}
	return result, nil
}

func eventCursor(r *http.Request, count int, version, epoch string) (int, error) {
	query, err := strictQuery(r, "after")
	if err != nil {
		return 0, err
	}
	raw, present := query["after"]
	headers := r.Header.Values("Last-Event-ID")
	if len(headers) > 1 || (present && len(headers) != 0) {
		return 0, errors.New("ambiguous cursor")
	}
	if len(headers) == 1 {
		raw, present = headers[0], true
	}
	if !present {
		return 0, nil
	}
	if version == "3" && raw != "0" {
		parts := strings.Split(raw, ":")
		if len(parts) != 2 || len(parts[0]) != 32 || len(parts[1]) == 0 || len(parts[1]) > 19 {
			return 0, errors.New("invalid cursor")
		}
		for _, c := range parts[0] {
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return 0, errors.New("invalid cursor")
			}
		}
		// Validate syntax before reporting a well-formed stale epoch. Never
		// silently reset: that could replay an externally visible action.
		for _, c := range parts[1] {
			if c < '0' || c > '9' {
				return 0, errors.New("invalid cursor")
			}
		}
		if parts[0] != epoch {
			return 0, store.ErrEpoch
		}
		raw = parts[1]
	}
	if raw == "" || len(raw) > 20 {
		return 0, errors.New("invalid cursor")
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid cursor")
		}
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || n > uint64(count) {
		return 0, errors.New("cursor beyond committed history")
	}
	return int(n), nil
}

func (s *Server) reserveStream(tenant string) bool {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	if s.streams >= maxStreams || s.streamCounts[tenant] >= maxTenantStreams {
		return false
	}
	if s.streamCounts == nil {
		s.streamCounts = make(map[string]int)
	}
	s.streams++
	s.streamCounts[tenant]++
	return true
}

func (s *Server) releaseStream(tenant string) {
	s.streamMu.Lock()
	defer s.streamMu.Unlock()
	s.streams--
	s.streamCounts[tenant]--
	if s.streamCounts[tenant] == 0 {
		delete(s.streamCounts, tenant)
	}
}

// Events replay committed lifecycle history, then wait for durable changes.
// Subscribe before reading: a commit racing the read closes this subscription,
// so the next read cannot silently miss it. Slow consumers never hold a DB tx.
func (s *Server) events(w http.ResponseWriter, r *http.Request, tenant string) {
	changed := s.Store.Changed()
	record, epoch, ok := s.recordSnapshot(w, r, tenant)
	if !ok {
		return
	}
	versions := r.Header.Values("AgentOS-Events-Version")
	version := "1"
	if len(versions) > 0 {
		version = versions[0]
	}
	if len(versions) > 1 || (version != "1" && version != "2" && version != "3") {
		problem(w, 400, "invalid_event_version", "unsupported event stream version")
		return
	}
	w.Header().Set("AgentOS-Events-Version", version)
	cursor, err := eventCursor(r, len(record.Events), version, epoch)
	if errors.Is(err, store.ErrEpoch) {
		problem(w, 409, "store_epoch_mismatch", "store restored; reconcile run history before resuming")
		return
	}
	if err != nil {
		problem(w, 400, "invalid_cursor", "invalid event cursor")
		return
	}
	w.Header().Set("AgentOS-Store-Epoch", epoch)
	if record.State.Terminal() && cursor == len(record.Events) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if _, ok := w.(http.Flusher); !ok {
		problem(w, 503, "streaming_unavailable", "event streaming unavailable")
		return
	}
	if !s.reserveStream(tenant) {
		w.Header().Set("Retry-After", "1")
		problem(w, 429, "stream_limit_exceeded", "event stream limit exceeded")
		return
	}
	defer s.releaseStream(tenant)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	rc := http.NewResponseController(w)
	frame := func(data []byte) error {
		if r.Context().Err() != nil {
			return r.Context().Err()
		}
		if err := rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if err := rc.Flush(); err != nil {
			return err
		}
		// HTTP/2 write deadlines reset idle streams too. Keep the finite bound
		// only around the write/flush; the heartbeat and lifetime bound idling.
		if err := rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return err
		}
		return nil
	}
	if err := frame([]byte(": connected\n\n")); err != nil {
		return
	}
	heartbeat := time.NewTicker(streamHeartbeat)
	defer heartbeat.Stop()
	expires := time.NewTimer(streamLifetime)
	defer expires.Stop()
	for sent := 0; ; {
		for cursor < len(record.Events) {
			select {
			case <-r.Context().Done():
				return
			case <-expires.C:
				return
			default:
			}
			payload := record.Events[cursor]
			if version == "1" {
				payload.ReasonCode = ""
			}
			event := struct {
				Sequence   int    `json:"sequence"`
				RunID      string `json:"run_id"`
				StoreEpoch string `json:"store_epoch,omitempty"`
				model.Event
			}{Sequence: cursor + 1, RunID: record.ID, Event: payload}
			id := strconv.Itoa(cursor + 1)
			if version == "3" {
				event.StoreEpoch = epoch
				id = epoch + ":" + id
			}
			encoded, err := json.Marshal(event)
			if err != nil {
				return
			}
			if err := frame([]byte(fmt.Sprintf("id: %s\nevent: run\ndata: %s\n\n", id, encoded))); err != nil {
				return
			}
			cursor++
			sent++
			if sent >= maxReplayEvents {
				return
			}
		}
		if record.State.Terminal() {
			end := map[string]any{"run_id": record.ID, "state": record.State, "last_event_id": cursor}
			if version == "3" {
				end["last_event_id"] = epoch + ":" + strconv.Itoa(cursor)
				end["store_epoch"] = epoch
			}
			encoded, _ := json.Marshal(end)
			_ = frame([]byte(fmt.Sprintf("event: end\ndata: %s\n\n", encoded)))
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-expires.C:
			return
		case <-heartbeat.C:
			if err := frame([]byte(": heartbeat\n\n")); err != nil {
				return
			}
		case <-changed:
			changed = s.Store.Changed()
			var observedEpoch string
			record, observedEpoch, err = s.Store.GetWithEpoch(tenant, record.ID)
			if err != nil || observedEpoch != epoch {
				return
			} // HTTP headers already sent; reconnect to observe failure.
		}
	}
}

func (s *Server) controllerHealthy() bool {
	if s.ControllerStatus == nil {
		return true
	} // Embedded users may manage reconciliation themselves.
	return healthyController(s.ControllerStatus())
}

func healthyController(status controller.Status) bool {
	return status.Running && !status.LastCompletedAt.IsZero() && time.Since(status.LastCompletedAt) <= 30*time.Second && status.ConsecutiveFailures == 0
}

func (s *Server) status(w http.ResponseWriter, r *http.Request, tenant string) {
	active, err := s.Store.Active()
	if err != nil {
		problem(w, 503, "storage_unavailable", "storage unavailable")
		return
	}
	states := make(map[model.State]int)
	count := 0
	for _, run := range active {
		if run.TenantID == tenant {
			count++
			states[run.State]++
		}
	}
	profile, err := s.Config.ResolvedRuntimeProfile()
	if err != nil {
		problem(w, 503, "environment_unavailable", "environment unavailable")
		return
	}
	result := map[string]any{"preview": s.Preview, "execution_enabled": !s.Preview,
		"runtime_profile": profile, "tenant_active": count, "tenant_limit": s.Config.MaxActivePerTenant,
		"active_by_state": states, "controller_monitored": s.ControllerStatus != nil,
		"event_stream_limit_per_tenant": maxTenantStreams}
	if s.ControllerStatus != nil {
		snapshot := s.ControllerStatus()
		result["controller"] = snapshot
		result["controller_healthy"] = healthyController(snapshot)
	}
	write(w, 200, result)
}
