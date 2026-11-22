package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nobelchowdary/agent-os/internal/model"
	"github.com/nobelchowdary/agent-os/internal/store"
)

func maintenanceDirectory(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func maintenanceFixture(t *testing.T, active bool) (string, model.Record) {
	t.Helper()
	path := filepath.Join(maintenanceDirectory(t), "state", "runs.db")
	db, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r, _, err := db.Admit(model.Record{ID: strings.Repeat("a", 32), TenantID: "tenant-a", Spec: model.Spec{Template: "python", Image: "registry.example/python@sha256:" + strings.Repeat("a", 64), RuntimeClass: "gvisor", Command: []string{"python", "-c", "print(1)"}, Resources: model.Resources{CPUMilli: 250, MemoryMiB: 256, DiskMiB: 128}, TimeoutSeconds: 30}}, "retained-key", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !active {
		r, err = db.TransitionWithCode(r.ID, []model.State{model.Queued}, model.Succeeded, "fixture completed", model.ReasonCompleted)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path, r
}

func TestStoreCommandDrainedBackupRestorePreservesIdentity(t *testing.T) {
	t.Setenv("AGENTOS_LOCAL_TOKEN", "") // Maintenance does not authenticate to the API.
	source, original := maintenanceFixture(t, false)
	backup := filepath.Join(maintenanceDirectory(t), "backup.db")
	var output bytes.Buffer
	if err := runStore([]string{"backup", "--db", source, "--output", backup}, &output); err != nil {
		t.Fatal(err)
	}
	var result struct {
		Operation string `json:"operation"`
		store.BackupInfo
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Operation != "backup" || result.Runs != 1 || result.ActiveRuns != 0 || len(result.SHA256) != 64 {
		t.Fatal("invalid maintenance result")
	}
	restored := filepath.Join(maintenanceDirectory(t), "restored.db")
	output.Reset()
	if err := runStore([]string{"restore", "--backup", backup, "--db", restored, "--sha256", result.SHA256}, &output); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(restored)
	if err != nil {
		t.Fatal(err)
	}
	r, created, err := db.Admit(model.Record{ID: strings.Repeat("b", 32), TenantID: original.TenantID, Spec: original.Spec}, "retained-key", 1, 1)
	if err != nil || created || r.ID != original.ID || r.ReasonCode != model.ReasonCompleted || len(r.Events) != len(original.Events) {
		t.Fatal("restore lost idempotency/history", err)
	}
	if _, err := db.Get("other-tenant", original.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("restore lost tenant scoping")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := runStore([]string{"check", "--db", restored}, &output); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || result.Operation != "check" || result.SHA256 == "" {
		t.Fatal("invalid check result", err)
	}
}

func TestStoreCommandRefusesUnsafeRecoveryAndSanitizesErrors(t *testing.T) {
	active, _ := maintenanceFixture(t, true)
	var output bytes.Buffer
	if err := runStore([]string{"check", "--db", active}, &output); err != nil {
		t.Fatal("inspection must allow active metadata", err)
	}
	destination := filepath.Join(maintenanceDirectory(t), "must-not-exist.db")
	err := runStore([]string{"backup", "--db", active, "--output", destination}, &output)
	var failure *storeCommandError
	if !errors.As(err, &failure) || failure.code != "active_runs_require_reconciliation" {
		t.Fatal("active backup not refused safely", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("failed backup published a file")
	}
	source, _ := maintenanceFixture(t, false)
	for _, args := range [][]string{{"restore", "--backup", source, "--db", destination, "--sha256", strings.Repeat("0", 64)}, {"check", "--db", filepath.Join(maintenanceDirectory(t), "private-missing-path")}, {"backup", "--db", source, "--output", source}} {
		err := runStore(args, &output)
		if err == nil || strings.Contains(err.Error(), source) || strings.Contains(err.Error(), "private-missing-path") {
			t.Fatal("unsafe or unsanitized maintenance failure", err)
		}
	}
	for _, args := range [][]string{{"unknown-private-value"}, {"check"}, {"backup", "--db", source}, {"restore", "--backup", source, "--db", destination}, {"check", "--db", source, "--unknown-private-flag"}, {"check", "--db", source, "extra"}} {
		if err := runStore(args, &output); err == nil || strings.Contains(err.Error(), source) || strings.Contains(err.Error(), "private-value") || strings.Contains(err.Error(), "private-flag") {
			t.Fatal("invalid arguments accepted/leaked", err)
		}
	}
	output.Reset()
	if err := runStore([]string{"--help"}, &output); err != nil || !strings.Contains(output.String(), "API must be stopped") {
		t.Fatal("maintenance help missing safety contract", err)
	}
}
