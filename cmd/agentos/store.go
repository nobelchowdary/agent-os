package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/nobelchowdary/agent-os/internal/store"
	bolt "go.etcd.io/bbolt"
)

const storeUsage = `Offline database maintenance (API must be stopped):
  agentos store check --db PATH
  agentos store backup --db PATH --output NEW_PATH
  agentos store restore --backup PATH --db NEW_PATH --sha256 EXPECTED_HEX

Paths must satisfy private-file checks. Backup/restore require no active runs.
Existing destination files are never replaced. Retain the reported SHA-256
separately and supply it on restore. No Kubernetes or provider access occurs.
`

type storeCommandError struct{ operation, code string }

func (e *storeCommandError) Error() string {
	return "store " + e.operation + ": " + e.code
}

func storeFailure(operation string, err error) error {
	code := "operation_failed"
	switch {
	case errors.Is(err, store.ErrSchema):
		code = "unsupported_schema"
	case errors.Is(err, store.ErrIntegrity):
		code = "integrity_check_failed"
	case errors.Is(err, store.ErrActive):
		code = "active_runs_require_reconciliation"
	case errors.Is(err, store.ErrChecksum):
		code = "checksum_mismatch"
	case errors.Is(err, store.ErrExists), errors.Is(err, os.ErrExist):
		code = "destination_exists"
	case errors.Is(err, os.ErrNotExist):
		code = "file_not_found"
	case errors.Is(err, os.ErrPermission):
		code = "permission_denied"
	case errors.Is(err, bolt.ErrTimeout):
		code = "store_busy_stop_api_first"
	}
	// Deliberately do not include raw paths, database values, or backend errors.
	return &storeCommandError{operation, code}
}

func runStore(args []string, output io.Writer) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		_, err := io.WriteString(output, storeUsage)
		return err
	}
	operation := args[0]
	if operation != "check" && operation != "backup" && operation != "restore" {
		return &storeCommandError{"command", "unknown_operation_use_store_help"}
	}
	flags := flag.NewFlagSet("store "+operation, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	db := flags.String("db", "", "database file (new destination for restore)")
	var destination, source, checksum string
	if operation == "backup" {
		flags.StringVar(&destination, "output", "", "new backup path")
	}
	if operation == "restore" {
		flags.StringVar(&source, "backup", "", "backup file")
		flags.StringVar(&checksum, "sha256", "", "trusted expected checksum")
	}
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, err = io.WriteString(output, storeUsage)
			return err
		}
		return &storeCommandError{operation, "invalid_arguments_use_store_help"}
	}
	if flags.NArg() != 0 || *db == "" || (operation == "backup" && destination == "") || (operation == "restore" && (source == "" || checksum == "")) {
		return &storeCommandError{operation, "missing_or_invalid_arguments_use_store_help"}
	}
	var info store.BackupInfo
	var err error
	switch operation {
	case "check":
		info, err = store.CheckOffline(*db)
	case "backup":
		info, err = store.BackupOffline(*db, destination)
	case "restore":
		info, err = store.RestoreOffline(source, *db, checksum)
	}
	if err != nil {
		return storeFailure(operation, err)
	}
	result := struct {
		Operation string `json:"operation"`
		store.BackupInfo
	}{operation, info}
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return fmt.Errorf("store %s: result_output_failed_inspect_destination_before_retry", operation)
	}
	return nil
}
