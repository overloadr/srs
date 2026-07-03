// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT

//go:build linux

package utils

import "testing"

func TestOpenFDStats(t *testing.T) {
	open, soft, ok := OpenFDStats()
	if !ok {
		t.Fatal("OpenFDStats should succeed on Linux")
	}
	if open <= 0 {
		t.Fatalf("open fd=%v, want > 0", open)
	}
	if soft == 0 {
		t.Fatalf("soft limit=%v, want > 0", soft)
	}
}
