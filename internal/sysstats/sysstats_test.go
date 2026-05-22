// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT
package sysstats

import (
	"context"
	"testing"
)

func TestCollect_ReturnsSnapshot(t *testing.T) {
	snap := Collect(context.Background())
	if snap == nil {
		t.Fatal("Collect returned nil")
	}
	if snap.CollectedAt.IsZero() {
		t.Fatal("CollectedAt should be set")
	}
	if snap.Process.PID <= 0 {
		t.Fatal("Process.PID should be set")
	}
}
