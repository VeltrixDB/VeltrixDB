//go:build !race

package main

// raceEnabled reports a -race build (tests skip their heaviest data sets).
const raceEnabled = false
