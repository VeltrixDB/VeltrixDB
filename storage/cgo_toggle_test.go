package storage

import "testing"

// The toggle is what keeps the -race CI job off the C++ path, so its parsing
// needs to be right — a silently-ignored value would reintroduce the timeout.
func TestCGOEngineDisabled_Parsing(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"1", true}, {"true", true}, {"TRUE", true}, {"t", true},
		{"yes", true}, {"on", true},
		{"0", false}, {"false", false}, {"", false}, {"garbage", false},
	}
	for _, tc := range cases {
		// Reset the sync.Once so each case is evaluated fresh.
		cgoDisabledOnce = onceReset()
		cgoDisabledVal = false
		if tc.val == "" {
			t.Setenv(CGOEngineDisabledEnv, "")
		} else {
			t.Setenv(CGOEngineDisabledEnv, tc.val)
		}
		if got := cgoEngineDisabled(); got != tc.want {
			t.Errorf("%s=%q → %v, want %v", CGOEngineDisabledEnv, tc.val, got, tc.want)
		}
	}
	// Leave it disabled=false for any later test in this package.
	cgoDisabledOnce = onceReset()
	cgoDisabledVal = false
}

// An engine must still build and serve with the toggle set — that is the
// configuration the -race job runs in.
func TestCGOEngineDisabled_EngineStillWorks(t *testing.T) {
	t.Setenv(CGOEngineDisabledEnv, "1")
	cgoDisabledOnce = onceReset()
	cgoDisabledVal = false
	t.Cleanup(func() { cgoDisabledOnce = onceReset(); cgoDisabledVal = false })

	se := transformTestEngine(t, t.TempDir())
	t.Cleanup(func() { se.Close() })

	if err := se.Put("toggle", []byte("value"), -1); err != nil {
		t.Fatalf("Put with cgo engine disabled: %v", err)
	}
	got, err := se.Get("toggle")
	if err != nil || string(got) != "value" {
		t.Fatalf("Get = %q, %v; want \"value\", nil", got, err)
	}
}

// The bridge is opt-in: anything but an explicit on/sqpoll must leave it off,
// and an unknown value must be reported rather than silently accepted.
func TestURingBridgeMode_Parsing(t *testing.T) {
	cases := []struct {
		val    string
		want   uringBridgeMode
		wantOK bool
	}{
		{"", uringBridgeOff, true}, {"off", uringBridgeOff, true}, {"0", uringBridgeOff, true},
		{"on", uringBridgeOn, true}, {"1", uringBridgeOn, true}, {"ON", uringBridgeOn, true},
		{"sqpoll", uringBridgeSQPoll, true}, {" SQPOLL ", uringBridgeSQPoll, true},
		{"fast", uringBridgeOff, false},
	}
	for _, tc := range cases {
		got, ok := parseURingBridgeMode(tc.val)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("parseURingBridgeMode(%q) = %v, %v; want %v, %v", tc.val, got, ok, tc.want, tc.wantOK)
		}
	}

	// The master switch wins over an explicit opt-in.
	t.Setenv(CGOEngineDisabledEnv, "1")
	t.Setenv(URingBridgeEnv, "sqpoll")
	cgoDisabledOnce = onceReset()
	cgoDisabledVal = false
	t.Cleanup(func() { cgoDisabledOnce = onceReset(); cgoDisabledVal = false })
	if m, _ := uringBridgeModeFromEnv(); m != uringBridgeOff {
		t.Errorf("with %s=1, mode = %v; want off", CGOEngineDisabledEnv, m)
	}
}
