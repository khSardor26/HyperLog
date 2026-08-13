Verified-By: BMAD Architect
Date: 2026-08-13
Output-Path: _bmad-output/ARCHITECTURE.md

# HyperLog — Technical Architecture

## Architecture Overview
HyperLog ingests high-volume log streams (disk or network), parses and normalizes records into structured events, maintains aggregations and alerting windows, and writes summaries/alerts to downstream systems. The architecture favors bounded-memory, streaming-first components using System.IO.Pipelines for efficient IO, System.Threading.Channels for stage decoupling and backpressure, Memory<T>/ReadOnlySequence<T> to avoid copies, and a configurable worker-pool model (Parallel/async workers) to scale predictably while avoiding OOM. Durability is provided via lightweight checkpointing and optional durable state stores for aggregation snapshots.

## 1. System components and responsibilities

- Ingestor
  - Sources: local files (tail), syslog/UDP/TCP, HTTP POSTs, Kafka.
  - Responsibilities: read bytes efficiently, write into a Pipe or into a bounded ingestion channel; maintain read offsets/positions for checkpointing; apply source-level backpressure if downstream is saturated.

- Chunker
  - Responsibilities: convert a continuous byte stream into "log chunks" (lines, multiline groups). Uses System.IO.Pipelines + ReadOnlySequence<byte> / SequenceReader<byte> to find delimiters without copying. Enforces MaxChunkSize to avoid memory blow-ups; oversized records are handled via DLQ or streaming-to-blob.

- Parser Workers
  - Responsibilities: parse chunk -> structured LogEvent (timestamp, level, message, fields). Use span-based UTF-8 parsing, reuse buffers from MemoryPool<byte>.Shared or ArrayPool<byte>. Reduce per-message allocations. Validate schema and enrich (IP resolving, hostname).

- Aggregation Engine
  - Responsibilities: maintain sliding/windowed aggregates, counters, histograms, alert rules. Keeps bounded state with eviction, approximate structures (HyperLogLog/Count-Min Sketch) where appropriate, and configurable retention windows. Periodically checkpoints snapshots to durable storage.

- Output Writers
  - Responsibilities: deliver aggregated summaries, alerts, or enriched events to sinks (HTTP, S3, Elasticsearch, Kafka). Implement retries, exponential backoff with jitter, idempotency, and circuit-breaker behavior.

- Persistence / State
  - Responsibilities: store checkpoints (file offsets, last-processed IDs), durable aggregator snapshots, and DLQ entries. Choices range from local write-ahead log + periodic snapshots to RocksDB/SQLite or object store (S3) for larger clusters.

- Observability / Telemetry
  - Responsibilities: expose Prometheus metrics, OpenTelemetry traces, and structured logs about pipeline state, latencies, memory, channel depths, error rates, and retry counts.

- Orchestration / Config
  - Responsibilities: service configuration knobs (worker counts, capacities, time windows, MaxLineSize), runtime health checks, liveness/readiness, and deployment via container images + k8s manifests.

## 2. Concurrency & Memory Model (detailed)

This section prescribes concrete patterns and knobs using .NET primitives.

Goals:
- Keep per-node memory bounded and predictable.
- Use streaming parsing to avoid copying large data.
- Apply backpressure to slow ingestion when downstream can't keep up.
- Prefer asynchronous (non-blocking) I/O and a small, fixed number of long-lived workers.

Key primitives:
- System.IO.Pipelines — efficient producer/consumer streaming of bytes. Use a single Pipe per input source or per ingestor task.
- System.Threading.Channels — stage-to-stage queues with bounded capacity to decouple producers/consumers and implement backpressure.
- Memory<T> / ReadOnlySequence<T> and SequenceReader<T> — parse across pipe buffer boundaries without copying. When handing a chunk to a worker that outlives the pipeline buffer, copy into a pooled IMemoryOwner<byte> (MemoryPool<byte>) to own the memory.
- Parallel/async workers: spawn long-lived Tasks for worker pools (preferred) or use Parallel.ForEachAsync for batch-style processing.

Patterns & recommended implementations:

1) Ingest -> Pipe
- Reader (file/socket) writes into a PipeWriter using GetMemory/Advance/FlushAsync.
- Configure PipeOptions with sensible thresholds:
  - pauseWriterThreshold: e.g., 64 KB
  - resumeWriterThreshold: e.g., 32 KB
- This keeps memory bounded per Pipe.

2) Chunker -> Channel<LogChunk>
- Chunker reads from PipeReader and uses SequenceReader<byte> to find newline(s). When a log chunk is complete:
  - If Parse workers will synchronously parse the sequence before advancing the reader, avoid copying.
  - If handing off to parser tasks, copy the chunk into pooled IMemoryOwner<byte> (MemoryPool<byte>.Shared.Rent) and publish a small struct:
    struct LogChunk { IMemoryOwner<byte> Buffer; int Length; SourceMeta Meta; }
- Publish via ChannelWriter<LogChunk>.WriteAsync which will wait when channel is full.

3) Channels (bounded) — use full-mode Wait by default
- Example creation:
  ```csharp
  var options = new BoundedChannelOptions(capacity)
  {
      FullMode = BoundedChannelFullMode.Wait,
      SingleReader = false,   // or true if you have a single parser reader
      SingleWriter = false,
      AllowSynchronousContinuations = false
  };
  var channel = Channel.CreateBounded<LogChunk>(options);
  ```
- Prefer FullMode.Wait to apply backpressure. For scenarios where stale logs may be safely dropped, consider DropOldest or bounded byte-capacity wrappers.

4) Ownership & lifetimes
- Do not hand ReadOnlySequence<byte> referencing Pipe buffers directly to worker tasks that will outlive the pipeline read lifetime. Either:
  - Parse synchronously before AdvanceTo() (works for small parse time), or
  - Copy into IMemoryOwner<byte> (MemoryPool) and transfer ownership to worker. Always call Dispose/Return after processing.

5) Buffering & pooling
- Use MemoryPool<byte>.Shared or custom pool; return buffers promptly.
- For small messages (< 4KB) prefer stackalloc/span parsing to avoid renting; for larger messages rent.

6) Parallel.ForEachAsync vs manual worker tasks
- For channel-based long-lived consumption, start N dedicated parser tasks:
  ```csharp
  for (int i=0;i<parserCount;i++)
     _ = Task.Run(ParserWorkerLoop);

  async Task ParserWorkerLoop()
  {
      await foreach (var chunk in channel.Reader.ReadAllAsync(ct))
      {
          Parse(chunk);
      }
  }
  ```
- Parallel.ForEachAsync is useful for partitioned batch workloads or IAsyncEnumerable producers, but a simple worker pool gives better control for metrics, cancellation, and graceful shutdown.

7) Channel sizing guidance (practical rules)
- Prefer capacity formula that targets a time-buffer, not just item count:
  - Let ingest_rate_bytes_per_sec (R), target_buffer_seconds (T) e.g., 2–5s, average_chunk_size_bytes (S).
  - capacity_items = ceil(R * T / S)
  - Example: R = 100 MB/s, T = 2s, S = 512 bytes -> capacity_items = 100e6 * 2 / 512 ≈ 390k items (too large for item-based channels)
- For item-count channels, use a conservative heuristic:
  - capacity = parserWorkerCount * 16 (small cushion)
  - OR implement a byte-budget wrapper: track total bytes currently held in channels; block producers when > byteBudget (e.g., 256MB–1GB).
- Recommended defaults:
  - For most deployments: channel capacity = parserCount * 32
  - Byte-budget: 256MB for channels on a 8GB node; 1GB on a 32GB node.

8) Backpressure strategies
- Default: BoundedChannelFullMode.Wait (strong backpressure). Works well for file tailing (producer naturally slows) and TCP (TCP receive window will apply backpressure).
- For UDP/instruments where backpressure isn't possible: implement a local drop strategy + metrics (Drop counters + DLQ).
- Monitoring: create Prometheus metrics for channel_length/usage and alert when > 80% capacity for N seconds.

9) Worker pool sizing strategies
- Parser workers (CPU-bound):
  - Start with parserCount = max(1, Environment.ProcessorCount - 1) to leave one core for GC/IO.
  - Tune by observing CPU utilization and parse latency p99. Aim for ~70-85% CPU utilization.
  - If parsing is lightweight and enrichment is the bottleneck, adjust accordingly.
- Aggregator workers (memory/state-bound):
  - Start with Environment.ProcessorCount / 2, increase if IO to external stores dominates.
- Output writers (IO-bound):
  - Use a small pool, e.g., 2–8 tasks with HttpClient connection pooling. Rely on async I/O instead of many threads.
- Dynamic autoscaling:
  - Implement runtime autoscaler based on CPU usage, queue length, and processing lag.

10) Avoiding OOM
- Hard caps and limits:
  - MaxLineSize: default 64 KiB (configurable). If a line exceeds this, route to DLQ or stream to blob store instead of attempting to allocate huge contiguous buffer.
  - Channel byte budget: observable, bounded (256MB–1GB).
  - Per-worker buffer caps: limit the number of rented buffers per worker to 1–2.
- Garbage Collection:
  - Prefer short-lived allocations and pooled buffers to reduce Gen0 churn.
  - Tune GC server mode for production (server GC) via environment variable: COMPlus_gcServer = true (or set in runtimeconfig).

Example patterns (pseudo-C#):

- Pipe-based reader:
```csharp
var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 64*1024, resumeWriterThreshold: 32*1024));
_ = Task.Run(async () => await FileToPipeLoop(fileStream, pipe.Writer, ct));
_ = Task.Run(async () => await ChunkerLoop(pipe.Reader, chunkChannel.Writer, ct));
```

- Chunker using SequenceReader:
```csharp
async Task ChunkerLoop(PipeReader reader, ChannelWriter<LogChunk> outWriter, CancellationToken ct)
{
  while (true)
  {
    var result = await reader.ReadAsync(ct);
    var buffer = result.Buffer;
    var seqReader = new SequenceReader<byte>(buffer);
    while (seqReader.TryReadTo(out ReadOnlySequence<byte> lineSeq, (byte)'\n'))
    {
      // copy into pooled buffer if handing to async parser
      var owner = MemoryPool<byte>.Shared.Rent((int)lineSeq.Length);
      lineSeq.CopyTo(owner.Memory.Span);
      await outWriter.WriteAsync(new LogChunk(owner, (int)lineSeq.Length), ct);
    }
    reader.AdvanceTo(buffer.GetPosition(0)); // consumed position set appropriately
    if (result.IsCompleted) break;
  }
}
```

## 3. Data flow (ASCII-art + numbered step flow)

Source(s) -> [Ingestor(s)] --pipe--> [Chunker(s)]
    |                                     |
    v                                     v
  (PipeReader)                        Channel<LogChunk> (bounded)
                                          |
                                   Parser worker pool (N)
                                          |
                                   Channel<ParsedEvent> (bounded)
                                          |
                                   Aggregation Engine (windowed)
                                          |
                      +---------------------+---------------------+
                      |                                           |
                 Output Writers                                Persistence
               (HTTP/Kafka/S3/ES)                              (checkpoints/DLQ)

Numbered step flow (single log line):

1. Ingestor reads bytes from file/socket using FileStream / Socket and writes into a PipeWriter (System.IO.Pipelines).
2. Chunker reads from PipeReader, locates newline(s) using SequenceReader<byte> / ReadOnlySequence<byte>, and extracts a log chunk.
3. If the parser pool will process asynchronously, the chunker copies the chunk into a pooled IMemoryOwner<byte> and publishes a LogChunk to a bounded Channel<LogChunk>. If the parser is synchronous/unblocked, parse inline.
4. Parser worker picks up LogChunk, parses UTF-8 bytes into LogEvent (timestamp, level, message, fields). Minimal allocations: use Span, Utf8JsonReader or custom parsing, and use pooled memory for temporary buffers.
5. Parser sends structured LogEvent to Aggregation Engine via Channel<ParsedEvent> or direct in-memory handoff.
6. Aggregation Engine updates windowed metrics/counters/histograms and evaluates alert rules. If an alert condition is met, enqueue an Alert for Output Writers.
7. Output Writers publish summaries/alerts to configured sinks with retries and idempotency keys. On success, Aggregation Engine may checkpoint state.
8. Periodic checkpointing persists read offsets and aggregator snapshots to durable storage. On restart, Ingestor resumes from last offset and Aggregation reloads snapshots.

## 4. Fault tolerance & error handling

- Poison messages
  - Keep a per-message retry counter. If parsing or processing fails N times (default N=3), route the message to a Dead Letter Queue (DLQ) persisted to disk or object store.
  - Metric: hyperlog_dlq_count_total with labels (source, reason).

- Retries + idempotency
  - Output Writers should use idempotent semantics (dedupe keys, idempotency keys), or rely on exactly-once sinks (rare). Implement exponential backoff with jitter (min 200ms, factor 2, max 30s).
  - Maintain per-output circuit breaker: after X consecutive failures, pause output writer and escalate.

- Checkpointing
  - Maintain two-level checkpointing:
    - Source offsets (file byte-offsets, Kafka offsets) persisted frequently (every 1–5s or after N messages).
    - Aggregation snapshots (state) persisted periodically (every 5–30s) and on graceful shutdown.
  - Use write-ahead log (WAL) for critical operations if strict durability is required.

- Partial failures and idempotency
  - Design outputs to be idempotent or store dedupe tokens (e.g., message ID + hash) so retries don't create duplicate alerts.

- Graceful shutdown
  - On SIGTERM: stop accepting new input, wait for channels to drain for gracePeriod (configurable), flush checkpoints, then exit.
  - Expose /health and /ready endpoints that reflect backlog and whether graceful shutdown is in progress.

- Observability for errors
  - Surface per-stage error rates and retries for fast detection; include example stack traces and chunk data (or chunk digest) in structured logs for debugging (but redact PII, see security section).

## 5. Performance targets & benchmarking guidance

Targets (example baseline; tune to your environment):
- Throughput target (per 8 vCPU, 32 GB node, SSD network):
  - Small-lines (avg 200 bytes): 100–200 MB/s (≈ 0.5–1.0M lines/sec)
  - Medium-lines (avg 1 KB): 40–100 MB/s
  - Large-lines (avg 16 KB): 10–40 MB/s
- Latency SLOs:
  - Parse-to-aggregate p50 < 50 ms, p99 < 500 ms
  - End-to-end alert delivery SLO (for critical alerts) < 2–5 seconds
- Memory budgets:
  - Parser worker: 0.1–8 MB per worker (typical < 1 MB for small-lines due to pooling)
  - Channel buffers: 256 MB–1 GB total across channels
  - Aggregator working state: depends on retention and cardinality; budget per node e.g., 4–16 GB for moderate cardinality workloads
- GC/Heap: keep Gen2 small enough to meet p99 GC pause < 100ms

Benchmark plan:
1. Build a log generator that can emit realistic log lines (distributions of line sizes, multiline stacks, tags).
2. Single-node microbench:
   - Measure parse throughput, CPU usage, RSS memory, channel lengths, GC timings (dotnet-counters/dotnet-trace).
   - Test with varied MaxLineSize and parser counts.
3. End-to-end:
   - Ingest via file tailing and TCP producers.
   - Measure end-to-end latency (ingest -> alert) percentiles and output sink latencies.
4. Stress tests:
   - Ramp ingestion by factors of 2x until backpressure actions trigger.
   - Introduce bursts and sustained loads; measure recovery time and DLQ behavior.
5. Failure injection:
   - Simulate sink failures, network partitions, and slow disks. Measure retry counts and checkpoint correctness.

Acceptance criteria (example):
- Sustains configured throughput for 10 minutes with p99 latency under SLO.
- No sustained queue growth beyond configured buffer for >2 minutes.
- Memory usage below configured container memory limit with headroom for GC.

## 6. Deployment & scaling

Single-node vertical scaling:
- Use server GC and set GC thread and threadpool settings for high-throughput. Example: run with environment variable DOTNET_gcServer = 1.
- Increase CPU and RAM as workload grows; scale parserCount and channel byte-budget proportionally.
- Node sizing guidance:
  - Dev: 2 vCPU / 4 GB
  - Small: 4 vCPU / 8–16 GB (up to ~50–100 MB/s)
  - Production: 8+ vCPU / 32–64 GB (100+ MB/s), adjust based on cardinality & aggregator memory

Multi-node horizontal scaling:
- Partitioning strategies:
  - Source-sharding: assign specific log sources (files, hosts) to nodes. Simple and effective.
  - Keyed-sharding: consistent hash on a key (e.g., tenant, host) and route parsed events to aggregator partitions (use Kafka or internal sharding).
  - Stateless parser nodes + central stream (Kafka) for aggregation: parser nodes produce to Kafka; aggregator group(s) consume and maintain per-partition state; use Kafka partitioning for exactly-once or at-least-once semantics with offsets.
- Coordinator patterns:
  - Use a message broker (Kafka) for multi-node reliable ingestion + replay.
  - For stateful aggregation, either co-locate aggregator with parser for local aggregates or centralize with a stream processing engine (Flink/ksql/ks) for stronger guarantees.

Containerization:
- Provide Docker image with runtime settings and readiness/liveness endpoints.
- Kubernetes manifests:
  - Requests and limits:
    - Example (production): requests: cpu: 4, memory: 16Gi; limits: cpu: 8, memory: 32Gi.
  - Affinity/anti-affinity:
    - Prefer anti-affinity for high-availability across nodes.
  - Persistent volumes:
    - For local checkpoints, use hostPath or PVC; prefer object store for long-term snapshots.
  - Horizontal Pod Autoscaler:
    - Scale on combined signals: CPU, custom metric (processing_lag_seconds), and channel_length.

Storage & durability:
- For per-source offsets and DLQ entries, use local fast storage (NVMe) with periodic backups to S3.
- Aggregator snapshot targets:
  - Small state: local snapshot + upload every checkpoint interval
  - Large state: use RocksDB or a dedicated key-value store to handle large cardinalities.

## 7. Observability: metrics, traces, logs & Prometheus

Instrument everything. Use OpenTelemetry for traces and a Prometheus exporter for metrics. Correlate traces with logs via trace-id.

Suggested metrics (Prometheus style):
- hyperlog_ingest_bytes_total (counter)
- hyperlog_ingest_rate_bytes (gauge or rate/histogram)
- hyperlog_channel_length{channel="chunk"} (gauge)
- hyperlog_channel_capacity{channel="chunk"} (gauge)
- hyperlog_channel_byte_usage{channel="chunk"} (gauge)
- hyperlog_parser_workers (gauge)
- hyperlog_parse_latency_seconds (histogram with buckets: 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s)
- hyperlog_parse_errors_total (counter)
- hyperlog_aggregator_state_size_bytes (gauge)
- hyperlog_output_retries_total (counter)
- hyperlog_dlq_count_total (counter)
- process_resident_memory_bytes (from process exporter)
- dotnet_gc_heap_size_bytes (if available)
- hyperlog_oom_events_total (counter) — increment on OOM-induced restart or detection

Example Prometheus alerting rules (simplified):

- High queue/backpressure
  - alert: HyperLogChannelBackpressure
    expr: |
      (hyperlog_channel_length{channel="chunk"} / max(hyperlog_channel_capacity{channel="chunk"}, 1))
      > 0.8
    for: 2m
    labels: { severity="warning" }
    annotations: { summary="Channel chunk >80% capacity for 2m", description="Check parser workers and memory." }

- Processing lag
  - alert: HyperLogProcessingLagHigh
    expr: hyperlog_processing_lag_seconds > 10
    for: 1m
    labels: { severity="critical" }

- Memory approaching container limit (use kube metrics)
  - alert: HyperLogOOMRisk
    expr: process_resident_memory_bytes > (container_memory_limit_bytes * 0.9)
    for: 2m
    labels: { severity="critical" }

- Parse error spike
  - alert: HyperLogParseErrorSpike
    expr: rate(hyperlog_parse_errors_total[5m]) > 0 and (rate(hyperlog_parse_errors_total[5m]) > 0.01 * rate(hyperlog_ingest_bytes_total[5m]))
    for: 5m
    labels: { severity="warning" }

Tracing:
- Spans: Ingestor.Read -> Chunker.Parse -> Aggregation.Update -> Output.Write
- Include attributes: source_id, chunk_size, sequence_id, parse_result (success/failure), aggregator_key.
- Capture trace-sampled payload (redact PII).

Logging:
- Structured JSON logs with fields: timestamp, level, component, message, source, trace_id, error.
- Log slow operations (parse latency > threshold), channel full events, and DLQ moves.

Dashboards:
- Channel lengths and capacities
- Parser CPU and parse latency histograms
- Aggregator memory usage and top keys by cardinality
- Output retry rates and success latency

## 8. Security considerations for log data

- PII scrubbing:
  - Implement configurable field-level scrubbing and regex-based redaction prior to persistent storage or outbound transmission. Provide allow-list and deny-list for fields.
  - Use deterministic hashing for identifiers when needed (e.g., user_id hashed with HMAC secret for correlation without exposing original data).
  - Keep scrubbing rules versioned and auditable.

- Encryption:
  - Transit: TLS 1.2+ for all network connections (HTTP, Kafka, S3).
  - At rest: encrypt checkpoints and DLQ on disk (filesystem encryption) or store in encrypted object store (S3 SSE).
  - Secrets: retrieve credentials from Vault/Secrets Manager; do not store secrets in plaintext or container images.

- Access control & auditing:
  - RBAC for operational actions (view DLQ, change retention).
  - Audit logs for configuration changes and alert silencing.

- Minimizing sensitive retention:
  - Default retention: store raw log bytes only in ephemeral local buffer; persist structured events with PII stripped. Short retention for raw payloads (e.g., 24–72 hours) unless explicitly required.

- Secure defaults:
  - Default MaxLineSize and DLQ policies to avoid accidental capture of very large payloads.
  - Data exfil protection: rate-limit outputs and monitor outbound traffic.

## 9. Open design decisions & recommended prototypes/spikes

These are areas that need a short spike to choose final implementation:

1. Aggregation state store
  - Option A: In-memory with periodic snapshots (fast, simple) — suitable for lower cardinality.
  - Option B: RocksDB/LMDB per node (durable local state) — better for high cardinality.
  - Option C: Centralized streaming engine (Kafka + Flink) — best for complex state & multi-node exactly-once semantics.
  - Spike: implement a small prototype (2–3 days) comparing memory and snapshot latency for options A/B given expected cardinality.

2. Multi-node scaling & partitioning
  - Option A: Source-shard workers + central sink (simpler)
  - Option B: Parsers publish to Kafka and aggregators are consumer groups (more durable & scalable)
  - Spike: build a parsers->Kafka->consumers prototype to measure end-to-end latency and rebalancing behavior.

3. Byte-budgeted channels vs item-count channels
  - Spike: implement a thin wrapper over Channel<T> that tracks current bytes and blocks when exceeding a byte budget. Measure impact on backpressure behavior.

4. Multiline log handling
  - Policy choices: "greedy combine until newline" vs heuristics (timestamp start detection). Spike: run multiline sample logs and measure correctness and memory usage.

5. DLQ storage & tooling
  - Decide between local disk vs S3-based DLQ. Spike: implement DLQ writing and reprocessing flow.

## 10. Configuration knobs (suggested keys & defaults)

Suggested appsettings.json / YAML example (values are starting points — tune in benchmarks):

{
  "HyperLog": {
    "MaxLineSizeBytes": 65536,
    "ParserWorkerCount": 0,               // 0 => auto: Environment.ProcessorCount - 1
    "AggregatorWorkerCount": 0,           // 0 => auto: max(1, ProcessorCount/2)
    "OutputWriterCount": 4,
    "Channel": {
      "ChunkCapacity": 512,               // count-based fallback
      "ChannelByteBudgetBytes": 268435456  // 256 MB
    },
    "CheckpointIntervalSeconds": 5,
    "Aggregation": {
      "WindowSeconds": 60,
      "RetentionSeconds": 300
    },
    "RetryPolicy": {
      "MaxAttempts": 5,
      "InitialBackoffMs": 200,
      "MaxBackoffMs": 30000
    },
    "MemoryPoolLimits": {
      "MaxRentedBytes": 1073741824  // 1 GB total across all rents (monitor & enforce)
    }
  }
}

## Implementation checklist (first 4-week sprint)

1. Skeleton pipeline:
  - Implement Ingestor -> Pipe -> Chunker -> Channel -> Parser worker pool -> simple aggregator -> console sink.
  - Add metrics for channel lengths, parse latency, and memory.

2. Bench harness:
  - Synthetic log generator with configurable line-size distribution and burst patterns.
  - Measurement harness to produce MB/s, latency percentiles and memory/GC stats.

3. DLQ + checkpointing:
  - Implement simple local DLQ and checkpoint persistence (file per source).
  - Support graceful restart/resume.

4. Observability:
  - Add Prometheus metrics and minimal dashboards.
  - Add OpenTelemetry traces for end-to-end path.

5. Security baseline:
  - Add redaction plugin hook and secrets integration stub.

---

If you'd like, I can write this Markdown file to _bmad-output/ARCHITECTURE.md for you now, or generate supporting skeleton code snippets or k8s manifests as the next step. Which would you prefer I do next?
