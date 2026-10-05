//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package storage

import (
	"errors"
	"os"
	"syscall"
)

// tryLockFile takes a non-blocking exclusive flock(2). flock locks belong to
// the open file description, so a second open+flock of the same file
// conflicts even from the same process — which is what makes a second
// in-process NewStorageEngine on one dir fail too.
func tryLockFile(f *os.File, _ string) error {
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.EWOULDBLOCK), errors.Is(err, syscall.EAGAIN):
			return errLockHeld
		case errors.Is(err, syscall.ENOLCK), errors.Is(err, syscall.EOPNOTSUPP):
			return errLockUnsupported
		default:
			return err
		}
	}
}

func unlockFile(f *os.File, _ string) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
