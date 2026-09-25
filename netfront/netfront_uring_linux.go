//go:build linux && cgo && go1.21 && !vxnf_nouring

package netfront

// Compiles the io_uring backend into netfront.cpp. Build with
// -tags vxnf_nouring on a Linux host without liburing to get the poll()
// backend only.

/*
#cgo linux CXXFLAGS: -DVXNF_HAVE_URING
#cgo linux LDFLAGS: -luring
*/
import "C"
