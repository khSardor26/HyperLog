Verified-By: BMAD Analyst
Date: 2026-08-13
Output-Path: _bmad-output/PRD.md

# HyperLog — Product Requirements Document

## Executive Summary
HyperLog is a .NET 10 high-throughput, multi-threaded log processing and analytics engine designed to stream, parse, aggregate and alert on multi‑GB log files in real time. It provides memory-safe ingestion (producer/consumer with bounded channels), zero/low-allocation chunked parsing (Memory<T>, ReadOnlySequence<T>, System.IO.Pipelines), configurable parallel processing (Parallel.ForEachAsync / task workers), windowed aggregation and anomaly detection, and structured JSON exports plus streaming alerts. The product targets operations, SRE and security teams that require reliable, low-latency metrics and anomaly notifications from large-scale log streams without large memory footprints.

## Problem Statement & Target Users
Problem
- Modern services produce multi‑GB log files or high-volume streams that are expensive or unreliable to process with naive in-memory or single-threaded tools. Key issues: OOM risk during bursts, high allocation overhead from naive parsing, and slow or non-deterministic aggregation/alerting latency.

Target users
- SRE / Ops engineers needing near-real-time service metrics and alerts.
- Security analysts needing streaming anomaly detection on logs.
- Platform/data engineers who export structured summaries for downstream analytics.

## Goals & Success Metrics (measurable)
Primary goals
- Reliable, bounded-memory ingestion of multi-GB logs.
- Low-latency streaming metrics and alerts (<2s for alerts in normal operating conditions).
- Scalable parallel parsing and aggregation across available CPU cores.

Success metrics (baseline / target)
- Throughput: sustain >= 100 MB/s ingest and parsing per instance on a 16‑vCPU / 32GB node; cluster scale to >500 MB/s via horizontal sharding.
- End-to-end alert latency: <= 2 seconds for windowed anomaly detection (1s or 5s window config).
- Processing completeness: 99.99% of lines parsed (excluding malformed lines) under load tests.
- Memory safety: no OOM during a 10 GB file ingest with default bounded queues; maximum resident set <= 8 GB on target node.
- Accuracy for basic anomaly detection: precision >= 95% and recall >= 90% on labeled synthetic datasets (initial target; adjust per model).
- Availability: 99.9% uptime for the service in production deployments.
- Export durability: JSON summaries written to durable store with at-least-once semantics; successful write rate >= 99.9%.

## Key Features (mapped to core requirements)
1. Producer-Consumer Architecture (maps to Core Req #1)
   - Bounded ingestion channels using System.Threading.Channels (BoundedChannelOptions) to provide backpressure and avoid OOM.
   - Configurable channel capacity by item count and by byte-budget (policy enforcement) with overflow policies: backpressure / drop-old / sample.
   - Per-source producer tokens and rate limiting to isolate noisy sources.

2. High-Throughput Chunking & Parsing (maps to Core Req #2)
   - Stream reading via System.IO.Pipelines for zero-copy I/O and PipeReader-based chunking.
   - In-memory parsing using Memory<T>, ReadOnlySequence<T>, Span<T>, and ArrayPool<T> to minimize allocations.
   - Utf8-first parsing approach with ReadOnlySequence<byte> to handle chunk boundaries and multi-line records.

3. Parallel Worker Pipeline (maps to Core Req #3)
   - Configurable degree of parallelism (Parallel.ForEachAsync over channel reader, or a pool of Task workers) to parse, filter, and transform records.
   - Keyed sharding option: preserve ordering per key by routing records to per-key worker queues.
   - Use of CancellationToken for graceful shutdown and window flush.

4. Aggregation Engine (maps to Core Req #4)
   - Real-time sliding and tumbling window support (1s, 5s, 1m etc.), per-window metric ops: counts, rates, histograms, percentiles.
   - Lightweight state containers using pooled buffers (ArrayPool<T>) and partitioned ConcurrentDictionary or lock-free structures for per-shard aggregation.
   - Threshold-based and statistical anomaly detectors (moving-average, z-score); plugin points for future ML models.

5. Output Writers (maps to Core Req #5)
   - Structured JSON streaming export using System.Text.Json.Utf8JsonWriter for low-allocation writes.
   - Streaming anomaly alerts via configurable sinks: HTTP webhook, Kafka, gRPC, or WebSocket; support at-least-once delivery and retry/backoff.
   - Configurable summary frequency and retention; atomic writes to durable storage (S3 / blob store / file) with safe roll/rotate.

6. Observability & Management
   - Built-in Prometheus metrics and structured logs (Microsoft.Extensions.Logging).
   - Health/readiness endpoints and runtime config API for tuning parallelism, channel sizes, and thresholds.

## Non-Functional Requirements
Performance
- Throughput: baseline >= 100 MB/s sustained ingest per 16-vCPU node; horizontally scalable.
- Latency: sliding-window metrics and alerts emitted within <= 2s of event ingestion for 1–5s windows.
- Parsing latency: average parse time per line target < 1 ms under load.

Memory & resource usage
- Bounded memory consumption enforced by bounded channels and configurable chunk sizes; default target max RSS <= 8 GB on a 16-core node during full-load tests.
- Use ArrayPool<T>, Memory<T>, and span-based parsing to minimize GC pressure.

Durability & correctness
- At-least-once processing semantics by default; application-level idempotence support or deduplication recommended for consumers.
- Checkpointing of file offsets or stream offsets to durable store for resume-after-crash behavior (local file, S3, or RocksDB-style store).

Security
- All network outputs (webhooks, APIs) require TLS; support mutual TLS for sensitive environments.
- AuthN/AuthZ on control and metrics endpoints (OAuth2 / mTLS / API keys), secrets from environment or vault.
- Optionally mask PII during parsing according to configurable rules.

Deployments & platforms
- Primary target: Linux x64 containers running .NET 10. Support Windows Server as secondary.
- Container images built from Microsoft .NET 10 base images. Kubernetes manifests and Helm chart for production deployment.
- CI: integration tests and performance benchmarks run in pipeline.

## User Stories & Acceptance Criteria (3–6)
1. Ingest and parse large file
   - Story: As an SRE, I can stream a 10 GB log file into HyperLog and obtain a complete parsed pass without OOM.
   - Acceptance: On a test node (16 vCPU / 32 GB RAM), ingest a 10 GB file end-to-end in <= 120s; resident memory <= 8 GB; no process OOM; all non-malformed lines parsed.

2. Real-time alerting
   - Story: As an incident responder, when error rate exceeds 5% over a 1-minute sliding window, I receive an alert.
   - Acceptance: Alert emitted and delivered to configured webhook within <= 2s after window closes; alert JSON includes window start/end, metric values, and top contributing keys; delivery retried until success (>=3 attempts) or moved to DLQ.

3. Parallel scaling
   - Story: As platform engineer, I can increase parallelism to improve throughput.
   - Acceptance: Doubling configured worker count increases throughput by >= 60% (until CPU saturation), validated by perf tests; no increased memory leak or OOM.

4. Durable export
   - Story: As data engineer, I can export periodic JSON summaries to durable storage for downstream analytics.
   - Acceptance: JSON summary written atomically to destination (S3 or local path) with timestamped filename; object contains top-level schema (window meta + metrics) and is verifiably complete.

5. Configurable backpressure
   - Story: As operator, I can configure channel capacity and overflow strategy.
   - Acceptance: Changing channel capacity and overflow strategy takes effect at runtime (or on next restart) and metrics reflect channel fullness/backpressure; system sheds load according to policy without OOM.

## Constraints & Assumptions
- Logs are newline-delimited text (UTF-8). Binary logs are out of scope for v1.
- Typical line size <= 64 KB. Lines > 1 MB are considered malformed and must be handled by configurable policy.
- Default deployment hardware: 16 vCPU / 32 GB RAM. Performance targets validated on this class of machine.
- Regex-based parsing may be CPU-bound; encourage field-extraction via span/string operations where possible.
- Exactly-once semantics are not a hard requirement for v1; at-least-once is acceptable with idempotent downstream consumers.

## Risks & Mitigations
- Risk: Unbounded memory consumption during bursts
  - Mitigation: Bounded channels (System.Threading.Channels), backpressure, disk-spooling fallback, and drop/sampling policies.

- Risk: Regex performance and catastrophic backtracking
  - Mitigation: Use compiled, anchored regex; prefer span/string tokenization where possible; provide safe regex linting in config UI.

- Risk: Slow downstream sinks causing internal queue buildup
  - Mitigation: Asynchronous buffered writers, retry with exponential backoff, circuit breaker and DLQ.

- Risk: Data loss on crash during aggregation
  - Mitigation: Periodic checkpointing of offsets and window state to durable storage; fast recovery path on restart.

- Risk: Alert storm (many alerts during transient)
  - Mitigation: Alert deduplication, rate limiting, and alert suppression windows.

## Open Questions
- Which persistent store is preferred for checkpointing and aggregation state (local FS, S3, RocksDB, Redis)?
- Which alert sinks must be supported first (webhook only, or Kafka/gRPC/WebSocket)?
- Exact production throughput SLO per customer — is the 100 MB/s baseline acceptable or should we target higher?
- Retention policy and aggregation storage TTL for summaries and intermediate state.
- Required compliance (PCI, HIPAA) or PII handling rules that influence masking.

## Recommended Next Steps (architecture & implementation follow-ups)
1. Architecture spike: produce a detailed component diagram showing producers, bounded channels, parser pool, aggregator shards, checkpoint store, and sinks. Call out failure modes and recovery flows.
2. Benchmark harness: build a synthetic log generator and perf harness to validate parsing throughput, memory behavior, and alert latency under controlled load (varying CPU/memory).
3. Storage decision: evaluate candidate persistent state stores for checkpointing (local file vs RocksDB vs Redis) and select by performance/durability trade-offs.

## Implementation Roadmap & Next 3 Milestones
Roadmap principle: ship a minimal, deterministic, well-tested MVP quickly; iterate on alert sophistication and scaling.

Milestone 1 — MVP Ingest & Parsing (2–4 weeks)
- Deliver core ingestion pipeline: System.IO.Pipelines reader, bounded System.Threading.Channels, chunk parsing with Memory<T>/ReadOnlySequence<T>, Parallel.ForEachAsync worker pool.
- Produce parsed JSON lines to local filesystem sink.
- Basic metrics (ingest rate, channel fullness) and health endpoints.
- Acceptance: pass baseline perf test (10 GB file, no OOM), unit tests for chunk boundary parsing.

Milestone 2 — Aggregation, Windows & Alerts (3–5 weeks)
- Implement windowed aggregation engine, sliding/tumbling windows, and simple anomaly detectors (threshold, moving-average).
- Implement streaming alert sink (HTTP webhook) and Utf8JsonWriter-based JSON summary export.
- Checkpointing of offsets to local durable store and graceful restart.
- Acceptance: alert latency <= 2s in test harness, successful checkpoint + resume.

Milestone 3 — Hardening & Productionization (3–6 weeks)
- Add observability (Prometheus metrics), configurable retention, container images, Kubernetes manifests/Helm.
- Implement retry/backoff, DLQ for alerts, secure endpoints (TLS/Auth).
- Run scale/perf tests; tune memory pools and GC settings; document operational playbook.
- Acceptance: validated on staging cluster at target throughput (100 MB/s) with monitoring alerts and runbook.

---

If you’d like, I can (pick one):
- run the architecture spike and produce a component diagram + API surface; or
- produce an initial benchmark plan and synthetic log generator; or
- draft the aggregation engine SPEC (window semantics + state model) ready for engineers.

Which would you like me to start next?
