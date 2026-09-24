// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT
package lb

import (
	"encoding/json"
	"strings"
)

// streamBinding locates the origin that owns a stream using identities that
// survive origin process restart. ServerID-ServiceID-PID changes on every
// SRS restart; device_id and listen IP do not.
type streamBinding struct {
	DeviceID  string `json:"device_id,omitempty"`
	IP        string `json:"ip,omitempty"`
	ServerKey string `json:"server_key,omitempty"`
}

func newStreamBinding(server *OriginServer, serverKey string) streamBinding {
	b := streamBinding{ServerKey: serverKey}
	if server != nil {
		b.DeviceID = server.DeviceID
		b.IP = server.IP
	}
	return b
}

func (b streamBinding) Empty() bool {
	return b.DeviceID == "" && b.IP == "" && b.ServerKey == ""
}

func (b streamBinding) Matches(server *OriginServer) bool {
	if server == nil {
		return false
	}
	if b.DeviceID != "" && server.DeviceID == b.DeviceID {
		return true
	}
	if b.IP != "" && server.IP == b.IP {
		return true
	}
	return false
}

func parseStreamBinding(raw []byte) streamBinding {
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return streamBinding{}
	}
	if strings.HasPrefix(s, "{") {
		var b streamBinding
		if err := json.Unmarshal([]byte(s), &b); err == nil && !b.Empty() {
			return b
		}
	}
	return streamBinding{ServerKey: s}
}

// SameStableOrigin reports whether a and b are the same physical origin
// across restarts (device_id, else IP, else ephemeral server ID).
func SameStableOrigin(a, b *OriginServer) bool {
	if a == nil || b == nil {
		return false
	}
	if a.DeviceID != "" && a.DeviceID == b.DeviceID {
		return true
	}
	if a.IP != "" && a.IP == b.IP {
		return true
	}
	return a.ID() != "" && a.ID() == b.ID()
}

func selectMatchingOrigin(servers []*OriginServer, binding streamBinding) *OriginServer {
	if binding.Empty() {
		return nil
	}
	var bestAlive, bestStale *OriginServer
	for _, server := range servers {
		if !binding.Matches(server) {
			continue
		}
		if IsOriginServerAlive(server) {
			if bestAlive == nil || server.UpdatedAt.After(bestAlive.UpdatedAt) {
				bestAlive = server
			}
			continue
		}
		if bestStale == nil || server.UpdatedAt.After(bestStale.UpdatedAt) {
			bestStale = server
		}
	}
	if bestAlive != nil {
		return bestAlive
	}
	return bestStale
}
