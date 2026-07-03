// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT

//go:build !linux

package utils

// OpenFDStats is only supported on Linux.
func OpenFDStats() (open int, softLimit uint64, ok bool) {
	return 0, 0, false
}
