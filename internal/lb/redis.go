// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT
package lb

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"srsx/internal/env"
	"srsx/internal/errors"
	"srsx/internal/logger"
	"srsx/internal/redisclient"
)

// redisLoadBalancer stores state in Redis.
type redisLoadBalancer struct {
	// The environment interface.
	environment env.ProxyEnvironment
	// The redis client.
	rdb redisclient.RedisClient
	// newClient is the factory used by Initialize to build the Redis client.
	// A struct field (rather than a package global) so concurrent tests can
	// each supply their own without racing on shared state.
	newClient func(addr, password string, db int) redisclient.RedisClient
	// keepaliveInterval is the period at which the default-backend keep-alive
	// goroutine re-Updates its registration. Struct field for test injection.
	keepaliveInterval time.Duration
}

// NewRedisLoadBalancer creates a new Redis-based load balancer.
func NewRedisLoadBalancer(environment env.ProxyEnvironment) OriginLoadBalancer {
	return &redisLoadBalancer{
		environment:       environment,
		newClient:         redisclient.New,
		keepaliveInterval: 30 * time.Second,
	}
}

func (v *redisLoadBalancer) Initialize(ctx context.Context) error {
	redisDatabase, err := strconv.Atoi(v.environment.RedisDB())
	if err != nil {
		return errors.Wrapf(err, "invalid PROXY_REDIS_DB %v", v.environment.RedisDB())
	}

	rdb := v.newClient(
		fmt.Sprintf("%v:%v", v.environment.RedisHost(), v.environment.RedisPort()),
		v.environment.RedisPassword(),
		redisDatabase,
	)
	v.rdb = rdb

	if err := rdb.Ping(ctx).Err(); err != nil {
		return errors.Wrapf(err, "unable to connect to redis %v", rdb.String())
	}
	logger.Debug(ctx, "RedisLB: connected to redis %v ok", rdb.String())

	server, err := NewDefaultOriginServerForDebugging(v.environment)
	if err != nil {
		return errors.Wrapf(err, "initialize default SRS")
	}

	if server != nil {
		if err := v.Update(ctx, server); err != nil {
			return errors.Wrapf(err, "update default SRS %+v", server)
		}

		// Keep alive.
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(v.keepaliveInterval):
					if err := v.Update(ctx, server); err != nil {
						logger.Warn(ctx, "update default SRS %+v failed, %+v", server, err)
					}
				}
			}
		}()
		logger.Debug(ctx, "RedisLB: Initialize default SRS media server, %+v", server)
	}
	return nil
}

func (v *redisLoadBalancer) Update(ctx context.Context, server *OriginServer) error {
	b, err := json.Marshal(server)
	if err != nil {
		return errors.Wrapf(err, "marshal server %+v", server)
	}

	key := v.redisKeyServer(server.ID())
	if err = v.rdb.Set(ctx, key, b, ServerAliveDuration).Err(); err != nil {
		return errors.Wrapf(err, "set key=%v server %+v", key, server)
	}
	if err = v.setIdentityIndexes(ctx, server, b); err != nil {
		return err
	}

	// Query all servers from redis, in json string.
	var serverKeys []string
	if b, err := v.rdb.Get(ctx, v.redisKeyServers()).Bytes(); err == nil {
		if err := json.Unmarshal(b, &serverKeys); err != nil {
			return errors.Wrapf(err, "unmarshal key=%v servers %v", v.redisKeyServers(), string(b))
		}
	}

	// Check each server expiration, if not exists in redis, remove from servers.
	for i := len(serverKeys) - 1; i >= 0; i-- {
		if _, err := v.rdb.Get(ctx, serverKeys[i]).Bytes(); err != nil {
			serverKeys = append(serverKeys[:i], serverKeys[i+1:]...)
		}
	}

	// Add server to servers if not exists.
	var found bool
	for _, serverKey := range serverKeys {
		if serverKey == key {
			found = true
			break
		}
	}
	if !found {
		serverKeys = append(serverKeys, key)
	}

	// Update all servers to redis.
	b, err = json.Marshal(serverKeys)
	if err != nil {
		return errors.Wrapf(err, "marshal servers %+v", serverKeys)
	}
	if err = v.rdb.Set(ctx, v.redisKeyServers(), b, 0).Err(); err != nil {
		return errors.Wrapf(err, "set key=%v servers %+v", v.redisKeyServers(), serverKeys)
	}

	return nil
}

func (v *redisLoadBalancer) List(ctx context.Context) ([]*OriginServer, error) {
	var serverKeys []string
	if b, err := v.rdb.Get(ctx, v.redisKeyServers()).Bytes(); err == nil {
		if err := json.Unmarshal(b, &serverKeys); err != nil {
			return nil, errors.Wrapf(err, "unmarshal key=%v servers %v", v.redisKeyServers(), string(b))
		}
	}

	var servers []*OriginServer
	for i := len(serverKeys) - 1; i >= 0; i-- {
		b, err := v.rdb.Get(ctx, serverKeys[i]).Bytes()
		if err != nil || len(b) == 0 {
			continue
		}
		var server OriginServer
		if err := json.Unmarshal(b, &server); err != nil {
			return nil, errors.Wrapf(err, "unmarshal key=%v server %v", serverKeys[i], string(b))
		}
		servers = append(servers, &server)
	}
	return servers, nil
}

func (v *redisLoadBalancer) Pick(ctx context.Context, streamURL string) (*OriginServer, error) {
	if server, err := v.Lookup(ctx, streamURL); err != nil {
		return nil, err
	} else if server != nil {
		_ = v.Bind(ctx, streamURL, server)
		return server, nil
	}

	return v.pickNewServer(ctx, streamURL, "")
}

func (v *redisLoadBalancer) Lookup(ctx context.Context, streamURL string) (*OriginServer, error) {
	return v.resolveBoundServer(ctx, streamURL)
}

func (v *redisLoadBalancer) Bind(ctx context.Context, streamURL string, server *OriginServer) error {
	if streamURL == "" || server == nil {
		return nil
	}
	payload, err := json.Marshal(newStreamBinding(server, v.redisKeyServer(server.ID())))
	if err != nil {
		return errors.Wrapf(err, "marshal binding for %v", streamURL)
	}
	key := v.redisKeyURL(streamURL)
	if err = v.rdb.Set(ctx, key, payload, 0).Err(); err != nil {
		return errors.Wrapf(err, "set key=%v binding %v", key, string(payload))
	}
	return nil
}

func (v *redisLoadBalancer) Repick(ctx context.Context, streamURL string, excludeServerID string) (*OriginServer, error) {
	v.rdb.Del(ctx, v.redisKeyURL(streamURL))
	return v.pickNewServer(ctx, streamURL, excludeServerID)
}

func (v *redisLoadBalancer) resolveBoundServer(ctx context.Context, streamURL string) (*OriginServer, error) {
	raw, err := v.rdb.Get(ctx, v.redisKeyURL(streamURL)).Bytes()
	if err != nil || len(raw) == 0 {
		return nil, nil
	}

	binding := parseStreamBinding(raw)
	if binding.Empty() {
		return nil, nil
	}

	if binding.DeviceID != "" {
		if server, err := v.loadServerJSON(ctx, v.redisKeyDevice(binding.DeviceID)); err != nil {
			return nil, err
		} else if server != nil {
			return server, nil
		}
	}
	if binding.IP != "" {
		if server, err := v.loadServerJSON(ctx, v.redisKeyIP(binding.IP)); err != nil {
			return nil, err
		} else if server != nil {
			return server, nil
		}
	}
	if binding.ServerKey != "" {
		if server, err := v.loadServerJSON(ctx, binding.ServerKey); err != nil {
			return nil, err
		} else if server != nil {
			return server, nil
		}
	}

	servers, err := v.List(ctx)
	if err != nil {
		return nil, err
	}
	return selectMatchingOrigin(servers, binding), nil
}

func (v *redisLoadBalancer) loadServerJSON(ctx context.Context, key string) (*OriginServer, error) {
	if key == "" {
		return nil, nil
	}
	b, err := v.rdb.Get(ctx, key).Bytes()
	if err != nil || len(b) == 0 {
		return nil, nil
	}
	var server OriginServer
	if err := json.Unmarshal(b, &server); err != nil {
		return nil, errors.Wrapf(err, "unmarshal key=%v server %v", key, string(b))
	}
	return &server, nil
}

func (v *redisLoadBalancer) pickNewServer(ctx context.Context, streamURL string, excludeServerID string) (*OriginServer, error) {
	var serverKeys []string
	if b, err := v.rdb.Get(ctx, v.redisKeyServers()).Bytes(); err == nil {
		if err := json.Unmarshal(b, &serverKeys); err != nil {
			return nil, errors.Wrapf(err, "unmarshal key=%v servers %v", v.redisKeyServers(), string(b))
		}
	}

	if len(serverKeys) == 0 {
		return nil, fmt.Errorf("no server available for %v", streamURL)
	}

	excludeKey := ""
	if excludeServerID != "" {
		excludeKey = v.redisKeyServer(excludeServerID)
	}

	order := append([]string(nil), serverKeys...)
	rand.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

	var firstUnmarshalErr error
	for _, tryServerKey := range order {
		if excludeKey != "" && tryServerKey == excludeKey {
			continue
		}
		b, err := v.rdb.Get(ctx, tryServerKey).Bytes()
		if err != nil || len(b) == 0 {
			continue
		}
		var server OriginServer
		if err := json.Unmarshal(b, &server); err != nil {
			if firstUnmarshalErr == nil {
				firstUnmarshalErr = errors.Wrapf(err, "unmarshal key=%v server %v", tryServerKey, string(b))
			}
			continue
		}
		return &server, nil
	}
	if firstUnmarshalErr != nil {
		return nil, firstUnmarshalErr
	}
	return nil, errors.Errorf("no server available in %v for %v", serverKeys, streamURL)
}

func (v *redisLoadBalancer) setIdentityIndexes(ctx context.Context, server *OriginServer, raw []byte) error {
	if server == nil || len(raw) == 0 {
		return nil
	}
	if server.DeviceID != "" {
		key := v.redisKeyDevice(server.DeviceID)
		if err := v.rdb.Set(ctx, key, raw, ServerAliveDuration).Err(); err != nil {
			return errors.Wrapf(err, "set key=%v device index", key)
		}
	}
	if server.IP != "" {
		key := v.redisKeyIP(server.IP)
		if err := v.rdb.Set(ctx, key, raw, ServerAliveDuration).Err(); err != nil {
			return errors.Wrapf(err, "set key=%v ip index", key)
		}
	}
	return nil
}

func (v *redisLoadBalancer) LoadHLSBySPBHID(ctx context.Context, spbhid string) (HLSPlayStream, error) {
	key := v.redisKeySPBHID(spbhid)

	b, err := v.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, errors.Wrapf(err, "get key=%v HLS", key)
	}

	// Store the raw JSON bytes that will be unmarshaled by the concrete type
	// The caller will need to handle the deserialization
	var actual map[string]interface{}
	if err := json.Unmarshal(b, &actual); err != nil {
		return nil, errors.Wrapf(err, "unmarshal key=%v HLS %v", key, string(b))
	}

	// Return nil for now - Redis LB needs the concrete type to properly deserialize
	// This is a limitation of using Redis with interfaces
	return nil, errors.Errorf("Redis load balancer cannot deserialize interface types")
}

func (v *redisLoadBalancer) LoadOrStoreHLS(ctx context.Context, streamURL string, value HLSPlayStream) (HLSPlayStream, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrapf(err, "marshal HLS %v", value)
	}

	key := v.redisKeyHLS(streamURL)
	if err = v.rdb.Set(ctx, key, b, HLSAliveDuration).Err(); err != nil {
		return nil, errors.Wrapf(err, "set key=%v HLS %v", key, value)
	}

	// Get SPBHID from value
	key2 := v.redisKeySPBHID(value.GetSPBHID())
	if err := v.rdb.Set(ctx, key2, b, HLSAliveDuration).Err(); err != nil {
		return nil, errors.Wrapf(err, "set key=%v HLS %v", key2, value)
	}

	// Return the same value since we just stored it
	return value, nil
}

func (v *redisLoadBalancer) StoreWebRTC(ctx context.Context, streamURL string, value RTCConnection) error {
	b, err := json.Marshal(value)
	if err != nil {
		return errors.Wrapf(err, "marshal WebRTC %v", value)
	}

	key := v.redisKeyRTC(streamURL)
	if err = v.rdb.Set(ctx, key, b, RTCAliveDuration).Err(); err != nil {
		return errors.Wrapf(err, "set key=%v WebRTC %v", key, value)
	}

	// Get Ufrag from value
	key2 := v.redisKeyUfrag(value.GetUfrag())
	if err := v.rdb.Set(ctx, key2, b, RTCAliveDuration).Err(); err != nil {
		return errors.Wrapf(err, "set key=%v WebRTC %v", key2, value)
	}

	return nil
}

func (v *redisLoadBalancer) LoadWebRTCByUfrag(ctx context.Context, ufrag string) (RTCConnection, error) {
	key := v.redisKeyUfrag(ufrag)

	b, err := v.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, errors.Wrapf(err, "get key=%v WebRTC", key)
	}

	// Return nil for now - Redis LB needs the concrete type to properly deserialize
	// This is a limitation of using Redis with interfaces
	var actual map[string]interface{}
	if err := json.Unmarshal(b, &actual); err != nil {
		return nil, errors.Wrapf(err, "unmarshal key=%v WebRTC %v", key, string(b))
	}

	return nil, errors.Errorf("Redis load balancer cannot deserialize interface types")
}

func (v *redisLoadBalancer) DeleteWebRTCByUfrag(ctx context.Context, ufrag string) error {
	key := v.redisKeyUfrag(ufrag)
	if b, err := v.rdb.Get(ctx, key).Bytes(); err == nil && len(b) > 0 {
		var meta struct {
			StreamURL string `json:"stream_url"`
		}
		if err := json.Unmarshal(b, &meta); err == nil && meta.StreamURL != "" {
			_ = v.rdb.Del(ctx, v.redisKeyRTC(meta.StreamURL))
		}
	}
	_ = v.rdb.Del(ctx, key)
	return nil
}

func (v *redisLoadBalancer) redisKeyURL(streamURL string) string {
	return fmt.Sprintf("srs-proxy-url:%v", streamURL)
}

func (v *redisLoadBalancer) redisKeyDevice(deviceID string) string {
	return fmt.Sprintf("srs-proxy-device:%v", deviceID)
}

func (v *redisLoadBalancer) redisKeyIP(ip string) string {
	return fmt.Sprintf("srs-proxy-ip:%v", ip)
}

func (v *redisLoadBalancer) redisKeyUfrag(ufrag string) string {
	return fmt.Sprintf("srs-proxy-ufrag:%v", ufrag)
}

func (v *redisLoadBalancer) redisKeyRTC(streamURL string) string {
	return fmt.Sprintf("srs-proxy-rtc:%v", streamURL)
}

func (v *redisLoadBalancer) redisKeySPBHID(spbhid string) string {
	return fmt.Sprintf("srs-proxy-spbhid:%v", spbhid)
}

func (v *redisLoadBalancer) redisKeyHLS(streamURL string) string {
	return fmt.Sprintf("srs-proxy-hls:%v", streamURL)
}

func (v *redisLoadBalancer) redisKeyServer(serverID string) string {
	return fmt.Sprintf("srs-proxy-server:%v", serverID)
}

func (v *redisLoadBalancer) redisKeyServers() string {
	return fmt.Sprintf("srs-proxy-all-servers")
}
