// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT
package sysstats

import (
	"context"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

// Snapshot is a point-in-time view of proxy host resource usage.
type Snapshot struct {
	CollectedAt time.Time      `json:"collected_at"`
	Host        HostInfo       `json:"host"`
	CPU         CPUInfo        `json:"cpu"`
	Memory      MemoryInfo     `json:"memory"`
	Swap        SwapInfo       `json:"swap"`
	Load        LoadInfo       `json:"load"`
	Network     NetworkInfo    `json:"network"`
	Process     ProcessInfo    `json:"process"`
}

type HostInfo struct {
	Hostname        string  `json:"hostname,omitempty"`
	OS              string  `json:"os,omitempty"`
	Platform        string  `json:"platform,omitempty"`
	PlatformFamily  string  `json:"platform_family,omitempty"`
	PlatformVersion string  `json:"platform_version,omitempty"`
	KernelVersion   string  `json:"kernel_version,omitempty"`
	Arch            string  `json:"arch,omitempty"`
	UptimeSeconds   uint64  `json:"uptime_seconds,omitempty"`
	BootTime        uint64  `json:"boot_time,omitempty"`
	Procs           uint64  `json:"procs,omitempty"`
}

type CPUInfo struct {
	LogicalCount int       `json:"logical_count"`
	Percent      float64   `json:"percent"`
	PerCPU       []float64 `json:"per_cpu,omitempty"`
}

type MemoryInfo struct {
	TotalBytes     uint64  `json:"total_bytes"`
	AvailableBytes uint64  `json:"available_bytes"`
	UsedBytes      uint64  `json:"used_bytes"`
	UsedPercent    float64 `json:"used_percent"`
}

type SwapInfo struct {
	TotalBytes  uint64  `json:"total_bytes"`
	UsedBytes   uint64  `json:"used_bytes"`
	UsedPercent float64 `json:"used_percent"`
}

type LoadInfo struct {
	Load1  float64 `json:"load1,omitempty"`
	Load5  float64 `json:"load5,omitempty"`
	Load15 float64 `json:"load15,omitempty"`
}

type NetworkInfo struct {
	Interfaces []NetInterfaceInfo `json:"interfaces,omitempty"`
	Total      NetCounterInfo     `json:"total"`
}

type NetInterfaceInfo struct {
	Name      string           `json:"name"`
	Addresses []string         `json:"addresses,omitempty"`
	Counters  NetCounterInfo   `json:"counters"`
}

type NetCounterInfo struct {
	BytesSent   uint64 `json:"bytes_sent"`
	BytesRecv   uint64 `json:"bytes_recv"`
	PacketsSent uint64 `json:"packets_sent"`
	PacketsRecv uint64 `json:"packets_recv"`
	Errin       uint64 `json:"errin,omitempty"`
	Errout      uint64 `json:"errout,omitempty"`
	Dropin      uint64 `json:"dropin,omitempty"`
	Dropout     uint64 `json:"dropout,omitempty"`
}

type ProcessInfo struct {
	PID            int     `json:"pid"`
	Goroutines     int     `json:"goroutines"`
	AllocBytes     uint64  `json:"alloc_bytes"`
	TotalAllocBytes uint64 `json:"total_alloc_bytes"`
	SysBytes       uint64  `json:"sys_bytes"`
	NumGC          uint32  `json:"num_gc"`
}

// Collect gathers host and proxy-process metrics. Partial failures are omitted
// rather than failing the whole snapshot.
func Collect(ctx context.Context) *Snapshot {
	snap := &Snapshot{CollectedAt: time.Now().In(time.Local)}

	if info, err := host.InfoWithContext(ctx); err == nil {
		snap.Host = HostInfo{
			Hostname:        info.Hostname,
			OS:              info.OS,
			Platform:        info.Platform,
			PlatformFamily:  info.PlatformFamily,
			PlatformVersion: info.PlatformVersion,
			KernelVersion:   info.KernelVersion,
			Arch:            runtime.GOARCH,
			UptimeSeconds:   info.Uptime,
			BootTime:        info.BootTime,
			Procs:           info.Procs,
		}
	}

	if counts, err := cpu.CountsWithContext(ctx, true); err == nil {
		snap.CPU.LogicalCount = counts
	}
	if perCPU, err := cpu.PercentWithContext(ctx, 0, true); err == nil {
		snap.CPU.PerCPU = perCPU
	}
	if total, err := cpu.PercentWithContext(ctx, 0, false); err == nil && len(total) > 0 {
		snap.CPU.Percent = total[0]
	}

	if vm, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		snap.Memory = MemoryInfo{
			TotalBytes:     vm.Total,
			AvailableBytes: vm.Available,
			UsedBytes:      vm.Used,
			UsedPercent:    vm.UsedPercent,
		}
	}
	if sm, err := mem.SwapMemoryWithContext(ctx); err == nil {
		snap.Swap = SwapInfo{
			TotalBytes:  sm.Total,
			UsedBytes:   sm.Used,
			UsedPercent: sm.UsedPercent,
		}
	}

	if avg, err := load.AvgWithContext(ctx); err == nil {
		snap.Load = LoadInfo{Load1: avg.Load1, Load5: avg.Load5, Load15: avg.Load15}
	}

	if counters, err := net.IOCountersWithContext(ctx, true); err == nil {
		for _, c := range counters {
			iface := NetInterfaceInfo{
				Name: c.Name,
				Counters: NetCounterInfo{
					BytesSent: c.BytesSent, BytesRecv: c.BytesRecv,
					PacketsSent: c.PacketsSent, PacketsRecv: c.PacketsRecv,
					Errin: c.Errin, Errout: c.Errout, Dropin: c.Dropin, Dropout: c.Dropout,
				},
			}
			snap.Network.Total.BytesSent += c.BytesSent
			snap.Network.Total.BytesRecv += c.BytesRecv
			snap.Network.Total.PacketsSent += c.PacketsSent
			snap.Network.Total.PacketsRecv += c.PacketsRecv
			snap.Network.Total.Errin += c.Errin
			snap.Network.Total.Errout += c.Errout
			snap.Network.Total.Dropin += c.Dropin
			snap.Network.Total.Dropout += c.Dropout
			snap.Network.Interfaces = append(snap.Network.Interfaces, iface)
		}
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	snap.Process = ProcessInfo{
		PID:             os.Getpid(),
		Goroutines:      runtime.NumGoroutine(),
		AllocBytes:      ms.Alloc,
		TotalAllocBytes: ms.TotalAlloc,
		SysBytes:        ms.Sys,
		NumGC:           ms.NumGC,
	}

	return snap
}
