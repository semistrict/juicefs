# shmipc Cache Server Benchmark Results

**Platform:** aarch64 Lima VM, 4 cores, 8GB RAM
**Date:** 2026-03-30
**Go:** go1.24.0
**Branch:** shared-cache-server

## 1. Unit Tests

20/20 PASS (0.47s total)

## 2. In-process Micro-benchmark (go test -bench)

Single-client cache miss round-trip within same process:

| Metric | Value |
|--------|-------|
| Per-op | 6,085 ns |
| Allocs | 4 allocs/op, 88 B/op |

## 3. Cross-process: Cache Miss Round-trip (single client)

| Spin Duration | Per-op | ops/s | Speedup vs old UDS (36μs) |
|---------------|--------|-------|---------------------------|
| 0 (no spin) | 30.6μs | 32,716 | 1.2x |
| 10μs | 4.8μs | 210,358 | 7.5x |
| **50μs** | **1.6μs** | **624,556** | **22.5x** |

## 4. Cross-process: Cache Hit (FD cached, single client, 4KB reads)

| Metric | Value |
|--------|-------|
| Per-op | **617 ns** |
| ops/s | 1,619,677 |
| Throughput | **6.2 GiB/s** |

Cache hits bypass shmipc entirely — just RLock + map lookup + pread.

## 5. Mixed Workload (80% hit, mixed read sizes 4K/64K/256K/1M, 4 workers)

| Mode | ops/s | Per-op | Throughput | Overhead |
|------|-------|--------|------------|----------|
| In-process (local cache) | 251,141 | 4.0μs | 30.4 GiB/s | — |
| Out-of-process (spin=50μs) | 194,477 | 5.1μs | 23.6 GiB/s | 1.1μs |

**77% of in-process throughput** with only 1.1μs overhead per op.

## 6. Multi-client Scaling (spin=50μs, 1 worker per client process)

| Client Processes | Per-client ops/s | Per-client latency | Total ops/s | Total throughput |
|------------------|-----------------|-------------------|-------------|-----------------|
| 1 | 76,414 | 13.1μs | 76,414 | 9.3 GiB/s |
| 2 | ~60,135 | 16.6μs | 120,270 | 14.6 GiB/s |
| 4 | ~34,370 | 29.1μs | 137,480 | 16.7 GiB/s |
| 8 | ~18,800 | 53.2μs | 150,400 | 18.3 GiB/s |

Total throughput scales ~2x from 1→8 clients on 4-core VM. Per-client latency degrades gracefully (no cliff). On production hardware with more cores, scaling would be better since spin-poll goroutines wouldn't compete with workers.

## 7. Latency Distribution (4 workers, spin=50μs)

| Mode | p50 | p90 | p99 | p999 | max |
|------|-----|-----|-----|------|-----|
| In-process | 5.1μs | 29μs | 116μs | 1.1ms | 5.8ms |
| Out-of-process | 4.4μs | 72μs | 197μs | 537μs | 3.6ms |

p50 is nearly identical (dominated by pread). Tail latency from GC pauses affects both modes equally.

## Key Design Decisions

1. **Persistent stream pool** (min(NumCPU, 4) streams) — avoids per-request stream creation overhead while allowing concurrent requests
2. **Zero-copy BufferReader/BufferWriter** with ReleasePreviousRead — shared memory buffers are recycled, not leaked
3. **Client-side FD cache fast path** — repeated reads bypass shmipc entirely (just pread on cached FD)
4. **Client ID sent once per stream** — eliminates string allocation on every request
5. **PollingSpinDuration patch** (forked shmipc-go) — consumer spins on shared memory queue instead of sleeping in epoll, eliminating notification syscalls in steady state
6. **recvFD uses ReadMsgUnix** — respects Go deadlines for clean shutdown (fixed hang-on-close bug)

## Allocation Profile (per cache miss round-trip)

4 allocs, 88 bytes:
- ReadString for cache key (Go string conversion)
- consistenthash.Get (cache manager internal)
- noneEviction.get (cache manager internal)
- protoResponse struct (client side)

The IPC layer itself is 0 allocs on the hot path.
