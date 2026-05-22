// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT
package proxy

import (
	"context"
	"fmt"
	"os"
	"time"

	"srsx/internal/env"
	"srsx/internal/lb"
	"srsx/internal/sysstats"
	"srsx/internal/version"
)

// ProxyStatusResponse is returned by GET /api/v1/proxy/status.
type ProxyStatusResponse struct {
	Code int              `json:"code"`
	PID  string           `json:"pid"`
	Data ProxyStatusData  `json:"data"`
}

type ProxyStatusData struct {
	Proxy    ProxyNodeInfo       `json:"proxy"`
	Backends []BackendNodeInfo   `json:"backends"`
}

type ProxyNodeInfo struct {
	Signature string             `json:"signature"`
	Version   string             `json:"version"`
	LoadBalancer string          `json:"load_balancer"`
	SystemAPI string             `json:"system_api"`
	Stats     *sysstats.Snapshot `json:"stats"`
}

type BackendNodeInfo struct {
	DeviceID  string   `json:"device_id,omitempty"`
	IP        string   `json:"ip"`
	APIPort   string   `json:"api_port,omitempty"`
	API       []string `json:"api,omitempty"`
	Alive     bool     `json:"alive"`
	ServerID  string   `json:"server_id,omitempty"`
	ServiceID string   `json:"service_id,omitempty"`
	PID       string   `json:"pid,omitempty"`
	UpdatedAt string   `json:"updated_at,omitempty"`
}

func buildProxyStatus(ctx context.Context, environment env.ProxyEnvironment, loadBalancer lb.OriginLoadBalancer) (*ProxyStatusResponse, error) {
	servers, err := loadBalancer.List(ctx)
	if err != nil {
		return nil, err
	}

	backends := make([]BackendNodeInfo, 0, len(servers))
	for _, server := range servers {
		if server == nil {
			continue
		}
		updatedAt := ""
		if !server.UpdatedAt.IsZero() {
			updatedAt = server.UpdatedAt.In(time.Local).Format(time.RFC3339Nano)
		}
		backends = append(backends, BackendNodeInfo{
			DeviceID:  server.DeviceID,
			IP:        server.IP,
			APIPort:   server.FirstAPIPort(),
			API:       server.API,
			Alive:     lb.IsOriginServerAlive(server),
			ServerID:  server.ServerID,
			ServiceID: server.ServiceID,
			PID:       server.PID,
			UpdatedAt: updatedAt,
		})
	}

	return &ProxyStatusResponse{
		Code: 0,
		PID:  fmt.Sprintf("%v", os.Getpid()),
		Data: ProxyStatusData{
			Proxy: ProxyNodeInfo{
				Signature:    version.Signature(),
				Version:      version.Version(),
				LoadBalancer: environment.LoadBalancerType(),
				SystemAPI:    environment.SystemAPI(),
				Stats:        sysstats.Collect(ctx),
			},
			Backends: backends,
		},
	}, nil
}
