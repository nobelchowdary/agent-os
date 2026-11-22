package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"
)

type BackupInfo struct {
	SchemaVersion uint64 `json:"schema_version"`
	SHA256        string `json:"sha256"`
	Runs          int    `json:"runs"`
	ActiveRuns    int    `json:"active_runs"`
	Bytes         int64  `json:"bytes"`
}

// privatePath rejects a symlink leaf or immediate parent. Canonicalizing older
// ancestors permits system aliases such as macOS /var without following a
// user-controlled database leaf. File opening separately checks inode identity.
func privatePath(path string, createParent bool) (string, error) {
	if path == "" {
		return "", errors.New("database path is required")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	dir := filepath.Dir(abs)
	if createParent {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return "", fmt.Errorf("create private database directory: %w", err)
		}
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return "", fmt.Errorf("inspect database directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return "", errors.New("database directory must be private (0700) and not a symlink")
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, filepath.Base(abs)), nil
}

func regularPrivateFile(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && info.Mode().Perm()&0077 == 0 && stat.Nlink == 1
}

// openDatabase never follows a source symlink or accepts an aliased hard link.
// Read-only opens never create a missing source and still obtain bbolt's lock,
// so a live API process causes a finite lock timeout rather than an unsafe copy.
func openDatabase(path string, readOnly bool) (*bolt.DB, *os.File, bool, error) {
	expected, err := os.Lstat(path)
	missing := errors.Is(err, os.ErrNotExist)
	if err != nil && (!missing || readOnly) {
		return nil, nil, false, err
	}
	if !missing && !regularPrivateFile(expected) {
		return nil, nil, false, errors.New("database file must be private, regular, and have one link")
	}
	if !missing && expected.Size() == 0 {
		return nil, nil, false, ErrIntegrity
	}
	var file *os.File
	fresh := false
	// bbolt otherwise flushes an unsynced freelist during writable Open,
	// before this package can reject an unsupported application schema.
	// initializeSchema restores normal freelist persistence after validating
	// the schema, without ever releasing the exclusive process lock. This is
	// separate from commit fsync (NoSync), which remains enabled throughout.
	opts := &bolt.Options{ReadOnly: readOnly, Timeout: time.Second, NoFreelistSync: !readOnly}
	opts.OpenFile = func(name string, flags int, mode os.FileMode) (*os.File, error) {
		flags |= syscall.O_NOFOLLOW
		if missing {
			flags |= os.O_EXCL
		}
		opened, err := os.OpenFile(name, flags, mode)
		if err != nil {
			return nil, err
		}
		actual, err := opened.Stat()
		if err != nil || !regularPrivateFile(actual) || (!missing && !os.SameFile(expected, actual)) {
			_ = opened.Close()
			return nil, errors.New("database file changed during open or is not private")
		}
		if !missing && actual.Size() == 0 {
			_ = opened.Close()
			return nil, ErrIntegrity
		}
		file = opened
		// Only our successful O_EXCL creation authorizes an empty application
		// schema. A pre-existing zero-bucket bbolt file is never a new store.
		fresh = missing
		return opened, nil
	}
	db, err := bolt.Open(path, 0600, opts)
	if err != nil {
		var pathErr *os.PathError
		if !errors.Is(err, bolt.ErrTimeout) && !errors.As(err, &pathErr) {
			return nil, nil, false, ErrIntegrity
		}
		return nil, nil, false, err
	}
	return db, file, fresh, nil
}

// CheckOffline validates a stopped database without modifying it. Active run
// records are reported for diagnosis; success does not make them restorable.
func CheckOffline(path string) (BackupInfo, error) {
	return inspectOffline(path, false, nil)
}

// BackupOffline requires a stopped and drained store. It publishes a new private
// file atomically, never overwrites a path, and leaves the source unchanged.
func BackupOffline(source, destination string) (BackupInfo, error) {
	return copyOffline(source, destination, "")
}

// RestoreOffline verifies the operator-provided checksum and all database/index
// invariants before publishing a new file. It never restores active metadata:
// replaying an old activation intent could duplicate an external effect.
func RestoreOffline(source, destination, expectedSHA256 string) (BackupInfo, error) {
	digest, err := hex.DecodeString(expectedSHA256)
	if err != nil || len(digest) != sha256.Size || len(expectedSHA256) != sha256.Size*2 {
		return BackupInfo{}, ErrChecksum
	}
	return copyOffline(source, destination, strings.ToLower(expectedSHA256))
}

func inspectOffline(path string, drained bool, destination io.Writer) (info BackupInfo, err error) {
	path, err = privatePath(path, false)
	if err != nil {
		return info, err
	}
	db, file, _, err := openDatabase(path, true)
	if err != nil {
		if errors.Is(err, bolt.ErrInvalid) || errors.Is(err, bolt.ErrInvalidMapping) || errors.Is(err, bolt.ErrChecksum) || errors.Is(err, bolt.ErrVersionMismatch) {
			return info, ErrIntegrity
		}
		return info, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	err = db.View(func(tx *bolt.Tx) error {
		var checkErr error
		// Check runs asynchronously; drain the channel even after a failure.
		for failure := range tx.Check() {
			if failure != nil {
				checkErr = ErrIntegrity
			}
		}
		if checkErr != nil {
			return checkErr
		}
		var validateErr error
		info, validateErr = validateSnapshot(tx)
		if validateErr != nil {
			return validateErr
		}
		if drained && info.ActiveRuns != 0 {
			return ErrActive
		}
		before, err := file.Stat()
		if err != nil || !regularPrivateFile(before) {
			return ErrIntegrity
		}
		hash := sha256.New()
		writer := io.Writer(hash)
		if destination != nil {
			writer = io.MultiWriter(destination, hash)
		}
		// The read-only database lock excludes supported writers for the entire
		// transaction/copy. Preserve exact bytes (including unused tail pages),
		// so the trusted checksum remains identical after a restore.
		n, err := io.Copy(writer, io.NewSectionReader(file, 0, before.Size()))
		if err != nil {
			return err
		}
		after, err := file.Stat()
		if err != nil || n != before.Size() || after.Size() != before.Size() || !regularPrivateFile(after) {
			return ErrIntegrity
		}
		current, err := os.Lstat(path)
		if err != nil || !os.SameFile(after, current) {
			return ErrIntegrity
		}
		info.SHA256 = hex.EncodeToString(hash.Sum(nil))
		info.Bytes = n
		return nil
	})
	return info, err
}

func copyOffline(source, destination, expected string) (BackupInfo, error) {
	return publishOffline(destination, func(file *os.File) (BackupInfo, error) {
		info, err := inspectOffline(source, true, file)
		if err == nil && expected != "" && info.SHA256 != expected {
			err = ErrChecksum
		}
		return info, err
	})
}

// A same-directory hard link is an atomic no-overwrite publication primitive.
// Until validation and fsync complete, the only name is the private temp file.
func publishOffline(destination string, write func(*os.File) (BackupInfo, error)) (info BackupInfo, err error) {
	destination, err = privatePath(destination, false)
	if err != nil {
		return info, err
	}
	if _, statErr := os.Lstat(destination); statErr == nil {
		return info, ErrExists
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return info, statErr
	}
	dir := filepath.Dir(destination)
	file, err := os.CreateTemp(dir, ".agentos-copy-*")
	if err != nil {
		return info, err
	}
	temp := file.Name()
	created, statErr := file.Stat()
	linked := false
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, file.Close())
		}
		if removeErr := os.Remove(temp); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
		err = errors.Join(err, syncDirectory(dir))
		if err != nil && linked {
			// Retain the original inode identity even after removing the temp
			// name. A final directory-fsync failure must also fail publication.
			current, currentErr := os.Lstat(destination)
			if created != nil && currentErr == nil && os.SameFile(created, current) {
				err = errors.Join(err, os.Remove(destination), syncDirectory(dir))
			}
		}
	}()
	if statErr != nil {
		return info, statErr
	}
	info, err = write(file)
	if err != nil {
		return info, err
	}
	if err = file.Sync(); err != nil {
		return info, err
	}
	err = file.Close()
	closed = true
	if err != nil {
		return info, err
	}
	// Independently validate the bytes that will be published, not only the
	// source transaction. No migration or repair is permitted on this path.
	copied, checkErr := CheckOffline(temp)
	if checkErr != nil {
		return info, checkErr
	}
	if copied != info || copied.ActiveRuns != 0 {
		return info, ErrIntegrity
	}
	current, statErr := os.Lstat(temp)
	if statErr != nil || !regularPrivateFile(current) || !os.SameFile(created, current) {
		return info, ErrIntegrity
	}
	if err = os.Link(temp, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return info, ErrExists
		}
		return info, err
	}
	linked = true
	if err = syncDirectory(dir); err != nil {
		return info, err
	}
	return info, nil
}

func syncDirectory(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	err = file.Sync()
	return errors.Join(err, file.Close())
}
