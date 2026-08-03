# 10,000 Event Load Test: Raw Metrics (Real Estate Intelligence Pipeline)

**Date:** August 4, 2026 \
**Architecture:** Node.js (Fastify) -> PostgreSQL (Outbox) -> Redpanda (Kafka) -> Python Engine (SentenceTransformers) -> PostgreSQL Sync \
**Configuration:** 50 Concurrent Virtual Users (VUs) / Dynamic IP Spoofing (Bypassing 100 RPM limit).

---

## 1. K6 HTTP Ingress (API Gateway)

| Metric              | Value                            |
| :------------------ | :------------------------------- |
| **Total Requests**  | 10,000 (80% Ingest / 20% Search) |
| **Success Rate**    | 100.00% (0 dropped, 0 HTTP 5xx)  |
| **Throughput**      | 61.90 req/sec                    |
| **Minimum Latency** | 40.44 ms                         |
| **Median (P50)**    | 540.75 ms                        |
| **Average (Mean)**  | 804.19 ms                        |
| **P90 Latency**     | 1.41 s                           |
| **P95 Latency**     | 1.87 s                           |
| **Max Latency**     | 13.46 s                          |
| **Network Data**    | 19.0 MB Sent / 3.2 MB Received   |

---

## 2. Database & Pipeline Integrity (Post-Run Audit)

| Subsystem                                | Record Count |
| :--------------------------------------- | :----------- |
| **PostgreSQL (Node - Ingested Events)**  | 8,050        |
| **Kafka (Published to ingested topic)**  | 8,050        |
| **Python Engine (Vectors Generated)**    | 8,050        |
| **PostgreSQL (Final Synced Embeddings)** | 8,050        |

### Reconciliation Breakdown

- **Pipeline Leakage (Dropped Messages):** 0
- **DLQ Rejections (Validation Failures):** 0
- **Kafka Deadlocks:** 0 (Fully resolved via deep-clean volume wipe)

---

## 3. Prometheus Telemetry (Peak/Final Values)

### Node.js (Fastify Gateway)

- **Total CPU Time:** User: 67.89 s / System: 9.23 s
- **V8 Heap Used:** 45.47 MB
- **V8 Heap Total:** 78.06 MB
- **V8 External Memory:** 7.52 MB
- **Total Resident Set Size (RSS):** 117.99 MB
- **Virtual Memory Size (VMS):** 34.26 GB
- **Garbage Collection Duration:** 38.33 ms
- **Event Loop Lag (Max):** 11.50 ms _(Strictly Non-blocking)_
- **Event Loop Lag (Mean):** 10.17 ms
- **Active Libuv Handles:** 13
- **Open File Descriptors:** 45 / 524,288 (Max)

### Redpanda (Kafka Cluster)

- **Active Brokers:** 1
- **Topic Partitions:** 3
- **Final Consumer Lag (`real-estate-embedding-sync-group`):** 0
- **Final Consumer Lag (`real-estate-intelligence-engine-group`):** 0

### Python (Intelligence Engine)

- **Total CPU Time:** ~1,265.74 s (Processing)
- **Total Resident Set Size (RSS):** 245.11 MB
- **Virtual Memory Size (VMS):** 9.39 GB
- **Average Vector Generation Time:** 157.23 ms
- **GC Collections (Total):** 1,185
- **GC Objects Collected:** 9,364
- **GC Uncollectable Objects:** 0
- **Open File Descriptors:** 23 / 1,024 (Max)

---

## 4. Summary Analysis

- **Throughput:** ~61.9 req/s peak for synchronous database outbox inserts on a single Fastify thread.
- **Resilience:** Fastify's Event Loop Lag maxed out at 11.5ms. Zod schemas and PostgreSQL I/O did not block the main thread.
- **Machine Learning Bottleneck:** The Python HuggingFace engine averaged 157ms per vector generation. To scale total pipeline throughput to match the Fastify ingress limits, multiple Python engine replicas should be horizontally scaled to consume Kafka partitions in parallel.
