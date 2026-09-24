// Copyright (c) 2026 Winlin
//
// SPDX-License-Identifier: MIT
package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"srsx/internal/lb"
	"srsx/internal/lb/lbfakes"
)

func TestShouldRetryOtherOrigin(t *testing.T) {
	if shouldRetryOtherOrigin(nil) {
		t.Fatal("nil should not retry")
	}
	if !shouldRetryOtherOrigin(errors.New("proxy stream to x failed, status=404 Not Found")) {
		t.Fatal("status error should retry")
	}
	if shouldRetryOtherOrigin(errors.New("copy stream to client")) {
		t.Fatal("copy error should not retry")
	}
}

type listOnlyLB struct {
	lb.OriginLoadBalancer
	servers []*lb.OriginServer
}

func (l listOnlyLB) List(context.Context) ([]*lb.OriginServer, error) {
	return l.servers, nil
}

func TestOtherLiveOrigins(t *testing.T) {
	skip := &lb.OriginServer{DeviceID: "d1", IP: "10.0.0.1", ServerID: "a", ServiceID: "s", PID: "1", UpdatedAt: time.Now()}
	other := &lb.OriginServer{DeviceID: "d2", IP: "10.0.0.2", ServerID: "b", ServiceID: "s", PID: "1", UpdatedAt: time.Now()}
	got := otherLiveOrigins(context.Background(), listOnlyLB{servers: []*lb.OriginServer{skip, other}}, skip)
	if len(got) != 1 || got[0].DeviceID != "d2" {
		t.Fatalf("got %+v", got)
	}
}

func TestStreamIdentityMatches(t *testing.T) {
	streamURL := "__defaultVhost__/live/airport1"
	if !streamIdentityMatches(streamURL, "live", "airport1", "") {
		t.Fatal("app/name should match")
	}
	if !streamIdentityMatches(streamURL, "", "", "/live/airport1") {
		t.Fatal("SRS default-vhost url should match")
	}
	if !streamIdentityMatches(streamURL, "", "", streamURL) {
		t.Fatal("raw streamURL should match")
	}
	if streamIdentityMatches(streamURL, "live", "other", "") {
		t.Fatal("different stream should not match")
	}
}

func TestPickStreamOriginUsesExistingBinding(t *testing.T) {
	bound := &lb.OriginServer{DeviceID: "d1", IP: "10.0.0.1", ServerID: "a", ServiceID: "s", PID: "1"}
	fake := &lbfakes.FakeOriginLoadBalancer{}
	fake.LookupReturns(bound, nil)

	got, err := pickStreamOrigin(context.Background(), fake, "__defaultVhost__/live/airport1")
	if err != nil {
		t.Fatal(err)
	}
	if got != bound {
		t.Fatalf("got %+v", got)
	}
	if fake.ListCallCount() != 0 || fake.PickCallCount() != 0 {
		t.Fatalf("Lookup hit must not discover or pick, list=%d pick=%d", fake.ListCallCount(), fake.PickCallCount())
	}
}

func TestPickStreamOriginDiscoversActivePublisher(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/streams" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"total":1,"streams":[{"name":"airport1","app":"live","url":"/live/airport1","publish":{"active":true}}]}`))
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	orig := originAPIClient
	originAPIClient = ts.Client()
	defer func() { originAPIClient = orig }()

	origin := &lb.OriginServer{
		IP: u.Hostname(), API: []string{u.Port()},
		DeviceID: "d1", ServerID: "a", ServiceID: "s", PID: "1",
		UpdatedAt: time.Now(),
	}
	wrong := &lb.OriginServer{
		IP: "127.0.0.1", API: []string{"1"},
		DeviceID: "d2", ServerID: "b", ServiceID: "s", PID: "1",
		UpdatedAt: time.Now(),
	}

	fake := &lbfakes.FakeOriginLoadBalancer{}
	fake.LookupReturns(nil, nil)
	fake.ListReturns([]*lb.OriginServer{wrong, origin}, nil)
	fake.PickReturns(wrong, nil)

	got, err := pickStreamOrigin(context.Background(), fake, "__defaultVhost__/live/airport1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.DeviceID != "d1" {
		t.Fatalf("got %+v, want discovered origin", got)
	}
	if fake.PickCallCount() != 0 {
		t.Fatalf("must not random-pick after discover, Pick=%d", fake.PickCallCount())
	}
	if fake.BindCallCount() != 1 {
		t.Fatalf("Bind=%d, want 1", fake.BindCallCount())
	}
}

func TestOriginPublishesStreamIgnoresInactive(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"streams":[{"name":"airport1","app":"live","url":"/live/airport1","publish":{"active":false}}]}`))
	}))
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	orig := originAPIClient
	originAPIClient = ts.Client()
	defer func() { originAPIClient = orig }()

	server := &lb.OriginServer{IP: u.Hostname(), API: []string{u.Port()}, UpdatedAt: time.Now()}
	if originPublishesStream(context.Background(), server, "__defaultVhost__/live/airport1") {
		t.Fatal("inactive publisher must not be treated as the live origin")
	}
}
