package main

import (
	"runtime"
	"testing"
)

func TestEnableContentionProfiles(t *testing.T) {
	prev := runtime.SetMutexProfileFraction(-1) // -1 reads without changing
	defer func() {
		runtime.SetMutexProfileFraction(prev)
		runtime.SetBlockProfileRate(0)
	}()

	enableContentionProfiles(0, 0)
	if got := runtime.SetMutexProfileFraction(-1); got != prev {
		t.Fatalf("fraction 0 changed the mutex profile rate: %d -> %d", prev, got)
	}

	enableContentionProfiles(7, 10000)
	if got := runtime.SetMutexProfileFraction(-1); got != 7 {
		t.Fatalf("mutex profile fraction = %d, want 7", got)
	}
	// SetBlockProfileRate has no getter, so the block rate is not asserted.
}
