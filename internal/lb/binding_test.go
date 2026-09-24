// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT
package lb

import (
	"testing"
	"time"
)

func TestParseStreamBinding_LegacyServerKey(t *testing.T) {
	got := parseStreamBinding([]byte("srs-proxy-server:a-b-1"))
	if got.ServerKey != "srs-proxy-server:a-b-1" || got.DeviceID != "" {
		t.Fatalf("got %+v", got)
	}
}

func TestParseStreamBinding_JSON(t *testing.T) {
	raw := []byte(`{"device_id":"origin-1","ip":"10.0.0.1","server_key":"srs-proxy-server:x"}`)
	got := parseStreamBinding(raw)
	if got.DeviceID != "origin-1" || got.IP != "10.0.0.1" {
		t.Fatalf("got %+v", got)
	}
}

func TestSameStableOrigin(t *testing.T) {
	a := &OriginServer{DeviceID: "d1", IP: "1.1.1.1", ServerID: "a", ServiceID: "s", PID: "1"}
	b := &OriginServer{DeviceID: "d1", IP: "1.1.1.1", ServerID: "b", ServiceID: "t", PID: "1"}
	c := &OriginServer{DeviceID: "d2", IP: "2.2.2.2", ServerID: "c", ServiceID: "u", PID: "1"}
	if !SameStableOrigin(a, b) {
		t.Fatal("same device should match across restart")
	}
	if SameStableOrigin(a, c) {
		t.Fatal("different device should not match")
	}
}

func TestSelectMatchingOrigin_PrefersNewestAlive(t *testing.T) {
	binding := streamBinding{DeviceID: "d1"}
	old := &OriginServer{DeviceID: "d1", ServerID: "old", PID: "1", UpdatedAt: time.Now().Add(-time.Minute)}
	fresh := &OriginServer{DeviceID: "d1", ServerID: "new", PID: "1", UpdatedAt: time.Now()}
	got := selectMatchingOrigin([]*OriginServer{old, fresh}, binding)
	if got != fresh {
		t.Fatalf("got %+v, want fresh", got)
	}
}
