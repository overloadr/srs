// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT

//go:build linux

package utils

import (
	"os"
	"sync"
	"syscall"
	"time"
)

const defaultFDStatsRefreshInterval = 30 * time.Second

var fdStatsRefreshInterval = defaultFDStatsRefreshInterval

var fdStatsState struct {
	softOnce  sync.Once
	softLimit uint64
	softOK    bool

	mu        sync.Mutex
	open      int
	openOK    bool
	refreshed time.Time
}

// OpenFDStats returns a fresh open FD count and soft RLIMIT_NOFILE.
func OpenFDStats() (open int, softLimit uint64, ok bool) {
	softLimit, softOK := fdSoftLimit()
	if !softOK {
		return 0, 0, false
	}

	open, openOK := readOpenFDCount()
	if !openOK {
		return 0, softLimit, false
	}

	return open, softLimit, true
}

// CachedOpenFDStats returns open FD count and soft limit with caching.
// forceRefresh bypasses the refresh interval and re-reads /proc/self/fd.
func CachedOpenFDStats(forceRefresh bool) (open int, softLimit uint64, ok bool) {
	softLimit, softOK := fdSoftLimit()
	if !softOK {
		return 0, 0, false
	}

	open, openOK := cachedOpenFDCount(forceRefresh)
	if !openOK {
		return 0, softLimit, false
	}

	return open, softLimit, true
}

func fdSoftLimit() (uint64, bool) {
	fdStatsState.softOnce.Do(func() {
		var lim syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
			return
		}
		fdStatsState.softLimit = lim.Cur
		fdStatsState.softOK = true
	})
	return fdStatsState.softLimit, fdStatsState.softOK
}

func readOpenFDCount() (int, bool) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, false
	}
	return len(entries), true
}

func cachedOpenFDCount(forceRefresh bool) (int, bool) {
	fdStatsState.mu.Lock()
	defer fdStatsState.mu.Unlock()

	stale := fdStatsState.refreshed.IsZero() ||
		time.Since(fdStatsState.refreshed) >= fdStatsRefreshInterval
	if !forceRefresh && !stale && fdStatsState.openOK {
		return fdStatsState.open, true
	}

	open, ok := readOpenFDCount()
	if !ok {
		if fdStatsState.openOK {
			return fdStatsState.open, true
		}
		return 0, false
	}

	fdStatsState.open = open
	fdStatsState.openOK = true
	fdStatsState.refreshed = time.Now()
	return open, true
}
