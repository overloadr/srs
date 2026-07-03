// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT

//go:build linux

package utils

import (
	"os"
	"syscall"
)

// OpenFDStats returns the current open file descriptor count and the soft
// RLIMIT_NOFILE on Linux. ok is false when stats cannot be read.
func OpenFDStats() (open int, softLimit uint64, ok bool) {
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		return 0, 0, false
	}

	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, lim.Cur, false
	}

	return len(entries), lim.Cur, true
}
