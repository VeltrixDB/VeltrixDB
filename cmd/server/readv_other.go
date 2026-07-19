//go:build !linux

package main

import "net"

func readvInto(_ net.Conn, _, _ []byte) int { return 0 }
func setSocketRecvBuf(_ net.Conn, _ int)    {}
