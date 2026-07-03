// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT

//go:build linux

package utils

import (
	"testing"
	"time"
)

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

func TestCachedOpenFDStats_UsesCacheWithinInterval(t *testing.T) {
	prev := fdStatsRefreshInterval
	fdStatsRefreshInterval = time.Minute
	t.Cleanup(func() { fdStatsRefreshInterval = prev })

	open1, soft1, ok1 := CachedOpenFDStats(true)
	if !ok1 {
		t.Fatal("expected cached stats")
	}

	open2, soft2, ok2 := CachedOpenFDStats(false)
	if !ok2 {
		t.Fatal("expected cached stats")
	}
	if open1 != open2 {
		t.Fatalf("open=%v cached=%v, want same value without refresh", open1, open2)
	}
	if soft1 != soft2 {
		t.Fatalf("soft=%v cached=%v, want same soft limit", soft1, soft2)
	}
}
