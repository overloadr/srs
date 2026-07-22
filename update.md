# Update Log

## 2026-07-22 — WebRTC UDP 会话空闲回收（修复 FD 泄漏）

### 背景

线上 `srs-proxy` 出现 `too many open files`，推流/HTTP Accept 全部失败。排查发现：

- `active_rtmp` 仅约几十，但进程 `open_fd` 达 **5000+**
- 其中绝大多数是 **UDP socket**，对端为 Origin RTC 端口（如 `8001/8002/8003`）
- 根因：WHEP/WHIP 媒体面 `backendUDP` **只创建不关闭**，会话索引也不淘汰，长时间运行后 FD 单调上涨

### 改动摘要

1. **空闲超时关闭**
   - 新增环境变量 `PROXY_WEBRTC_IDLE_TIMEOUT`，默认 `120s`
   - 有 UDP 收发则重置定时器；超时后关闭会话

2. **`rtcConnection.Close()`**
   - 取消会话 ctx（停止 backend→client reader）
   - 关闭并清空 `backendUDP`
   - 回调反注册缓存与 LB
   - `sync.Once`，可安全重复调用

3. **缓存与 LB 清理**
   - 从 `usernames`、`addresses` 删除会话
   - LB 新增 `DeleteWebRTCByUfrag`（memory / redis）

4. **其它加固**
   - `connectBackend` 增加 `dialMu`，避免并发双 dial 泄漏 FD
   - 增加 `active_webrtc` 相关 Debug 日志
   - Proxy `Close()` 时关闭全部 WebRTC 会话

### 涉及文件

- `internal/proxy/rtc.go` / `rtc_test.go`
- `internal/lb/lb.go` / `mem.go` / `redis.go`（及 fakes、测试）
- `internal/env/env.go` / `env_test.go`（及 fakes）

### 配置

```bash
# 可选，默认 120s
export PROXY_WEBRTC_IDLE_TIMEOUT=120s
```

### 验证建议

```bash
# 重启 proxy 后观察 FD / UDP
watch -n 2 'echo open_fd=$(ls /proc/$(pgrep -n -f srs-proxy)/fd | wc -l); ss -uanp | grep -c srs-proxy'
```

停掉 WHEP/WHIP 后约 `PROXY_WEBRTC_IDLE_TIMEOUT` 时间内，`open_fd` 与 UDP 数应随会话下降，而不再只涨不跌。

### 影响说明

- 对外 WHIP/WHEP SDP 交互不变
- 长时间无媒体流量的会话会被回收，客户端需重新建连
- 同流多路 WHEP 按 **ufrag/会话** 回收，不会因关一路误伤其它路
