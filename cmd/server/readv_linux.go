//go:build linux

package main

import (
	"net"
	"syscall"
	"unsafe"
)

// readvInto calls readv(2) on the underlying TCP socket to scatter-read into
// up to two contiguous segments of a single pre-allocated buffer.  This lets
// a caller fill a ring-buffer in a single syscall when the write pointer has
// wrapped to the beginning of the buffer.
//
// buf0 and buf1 are the two segments (buf1 may be empty).  Returns total bytes
// read, or 0 on EAGAIN / EWOULDBLOCK (nothing buffered yet).  All other errors
// return 0 so the caller falls back to the normal read path.
func readvInto(conn net.Conn, buf0, buf1 []byte) int {
	tc, ok := conn.(*net.TCPConn)
	if !ok || len(buf0) == 0 {
		return 0
	}

	rc, err := tc.SyscallConn()
	if err != nil {
		return 0
	}

	var total int
	_ = rc.Read(func(fd uintptr) bool {
		iovCount := 1
		iov := [2]syscall.Iovec{
			{Base: &buf0[0], Len: uint64(len(buf0))},
		}
		if len(buf1) > 0 {
			iov[1] = syscall.Iovec{Base: &buf1[0], Len: uint64(len(buf1))}
			iovCount = 2
		}
		n, _, errno := syscall.RawSyscall(
			syscall.SYS_READV,
			fd,
			uintptr(unsafe.Pointer(&iov[0])),
			uintptr(iovCount),
		)
		if errno == syscall.EAGAIN || errno == syscall.EWOULDBLOCK || errno != 0 {
			return true
		}
		total = int(n)
		return true
	})
	return total
}

// setSocketRecvBuf sets SO_RCVBUF on a TCP connection to hint the kernel to
// use a larger receive buffer.  A larger buffer lets the kernel stage more
// data before the user-space reader wakes, increasing the effective burst size
// delivered per read(2) call and reducing round-trips through the TCP stack.
//
// The kernel doubles the requested value to account for bookkeeping overhead
// (see tcp(7)), so requesting 256 KB yields ~512 KB actual buffer.
// Call this once right after accept().
func setSocketRecvBuf(conn net.Conn, bytes int) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	rc, err := tc.SyscallConn()
	if err != nil {
		return
	}
	_ = rc.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, bytes)
	})
}
