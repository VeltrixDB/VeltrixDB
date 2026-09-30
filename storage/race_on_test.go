//go:build race

package storage

// raceEnabled lets heavy, single-threaded quality tests skip under -race
// (5–10× slower there, with nothing concurrent to check); CI runs them in
// the search job without -race.
const raceEnabled = true
