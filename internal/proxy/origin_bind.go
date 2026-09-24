// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT
package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"srsx/internal/lb"
	"srsx/internal/logger"
	"srsx/internal/utils"
)

const originStreamDiscoverTimeout = 2 * time.Second

// originAPIClient queries origin HTTP API for stream discovery. Tests may replace it.
var originAPIClient = &http.Client{Timeout: originStreamDiscoverTimeout}

func bindStreamOrigin(ctx context.Context, loadBalancer lb.OriginLoadBalancer, streamURL string, server *lb.OriginServer) {
	if loadBalancer == nil || streamURL == "" || server == nil {
		return
	}
	if err := loadBalancer.Bind(ctx, streamURL, server); err != nil {
		logger.Warn(ctx, "bind stream %v to backend %+v failed, %+v", streamURL, server, err)
	}
}

// pickStreamOrigin resolves where a play request should go.
// Existing Redis/memory bindings win; if Redis was flushed while publishers
// stayed connected, it asks each origin /api/v1/streams who is actually
// publishing, binds that origin, and only then falls back to a random Pick.
func pickStreamOrigin(ctx context.Context, loadBalancer lb.OriginLoadBalancer, streamURL string) (*lb.OriginServer, error) {
	if loadBalancer == nil {
		return nil, fmt.Errorf("no load balancer")
	}
	if bound, err := loadBalancer.Lookup(ctx, streamURL); err != nil {
		return nil, err
	} else if bound != nil {
		return bound, nil
	}

	if found := discoverOriginForStream(ctx, loadBalancer, streamURL); found != nil {
		logger.Warn(ctx, "discovered stream %v on backend %+v", streamURL, found)
		bindStreamOrigin(ctx, loadBalancer, streamURL, found)
		return found, nil
	}

	return loadBalancer.Pick(ctx, streamURL)
}

func discoverOriginForStream(ctx context.Context, loadBalancer lb.OriginLoadBalancer, streamURL string) *lb.OriginServer {
	servers, err := loadBalancer.List(ctx)
	if err != nil || len(servers) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, originStreamDiscoverTimeout)
	defer cancel()

	found := make(chan *lb.OriginServer, 1)
	var wg sync.WaitGroup
	for _, server := range servers {
		if server == nil || !lb.IsOriginServerAlive(server) {
			continue
		}
		wg.Add(1)
		go func(server *lb.OriginServer) {
			defer wg.Done()
			if !originPublishesStream(ctx, server, streamURL) {
				return
			}
			select {
			case found <- server:
			default:
			}
		}(server)
	}
	go func() {
		wg.Wait()
		close(found)
	}()

	select {
	case server, ok := <-found:
		if ok {
			return server
		}
		return nil
	case <-ctx.Done():
		return nil
	}
}

type originStreamsResponse struct {
	Code    int `json:"code"`
	Total   int `json:"total"`
	Streams []struct {
		Name    string `json:"name"`
		App     string `json:"app"`
		URL     string `json:"url"`
		Publish struct {
			Active bool `json:"active"`
		} `json:"publish"`
	} `json:"streams"`
}

func originPublishesStream(ctx context.Context, server *lb.OriginServer, streamURL string) bool {
	apiPort := server.FirstAPIPort()
	if apiPort == "" || server.IP == "" {
		return false
	}

	for start := 0; start < 10000; {
		reqURL := fmt.Sprintf("http://%s:%s/api/v1/streams?start=%d&count=100", server.IP, apiPort, start)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return false
		}
		resp, err := originAPIClient.Do(req)
		if err != nil {
			return false
		}
		var body originStreamsResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if decodeErr != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		for i := range body.Streams {
			stream := body.Streams[i]
			if !streamIdentityMatches(streamURL, stream.App, stream.Name, stream.URL) {
				continue
			}
			if stream.Publish.Active {
				return true
			}
		}
		got := start + len(body.Streams)
		if len(body.Streams) == 0 || (body.Total > 0 && got >= body.Total) || len(body.Streams) < 100 {
			return false
		}
		start = got
	}
	return false
}

func streamIdentityMatches(streamURL, app, name, rawURL string) bool {
	if streamURL == "" {
		return false
	}
	if app != "" && name != "" {
		joined := "__defaultVhost__/" + strings.Trim(app, "/") + "/" + strings.Trim(name, "/")
		if joined == streamURL {
			return true
		}
	}
	if rawURL == "" {
		return false
	}
	if rawURL == streamURL {
		return true
	}
	path := rawURL
	if !strings.Contains(path, "://") {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		path = "http://origin" + path
	}
	built, err := utils.BuildStreamURL(path)
	return err == nil && built == streamURL
}

func otherLiveOrigins(ctx context.Context, loadBalancer lb.OriginLoadBalancer, skip *lb.OriginServer) []*lb.OriginServer {
	if loadBalancer == nil {
		return nil
	}
	servers, err := loadBalancer.List(ctx)
	if err != nil || len(servers) == 0 {
		return nil
	}
	out := make([]*lb.OriginServer, 0, len(servers))
	for _, server := range servers {
		if server == nil {
			continue
		}
		if skip != nil && (server.ID() == skip.ID() || lb.SameStableOrigin(server, skip)) {
			continue
		}
		if !lb.IsOriginServerAlive(server) {
			continue
		}
		out = append(out, server)
	}
	return out
}

func shouldRetryOtherOrigin(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "failed, status=") ||
		strings.Contains(s, "do request to") ||
		strings.Contains(s, "do DELETE to") ||
		strings.Contains(s, "no http") ||
		strings.Contains(s, "no udp server")
}
