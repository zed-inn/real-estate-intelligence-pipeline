# Real Estate Intelligence Pipeline

![Node.js](https://img.shields.io/badge/Node.js-Fastify-339933?style=flat-square&logo=node.js)
![Python](https://img.shields.io/badge/Python-SentenceTransformers-3776AB?style=flat-square&logo=python)
![Redpanda](https://img.shields.io/badge/Redpanda-Kafka_Compatible-FF6600?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-pgvector_HNSW-4169E1?style=flat-square&logo=postgresql)

A production-grade, event-driven semantic search pipeline for real estate. POST a property's structured attributes — BHK, price, amenities, proximity distances, livability scores. The pipeline converts them into a semantic vector. When a user searches _"quiet gated 3BHK near metro with a gym"_, they get back listings ranked by meaning, not keyword overlap.

The hard part isn't the search. It's getting 101 typed fields to respond correctly to unstructured natural-language queries, across a polyglot TypeScript/Python boundary, with zero event loss if any service goes down mid-flight.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [System Architecture](#system-architecture)
- [Ingestion Lifecycle](#ingestion-lifecycle)
- [Tech Stack & Engineering Decisions](#tech-stack--engineering-decisions)
- [Concurrency & Data Integrity](#concurrency--data-integrity)
- [Performance & Load Testing](#performance--load-testing)
- [Design Tradeoffs](#design-tradeoffs)
- [Infrastructure Hardening](#infrastructure-hardening)
- [Observability](#observability)
- [Engineering Journey](#engineering-journey)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Roadmap](#roadmap)

---

## Problem Statement

Natural-language property search is an unsolved problem for most real estate platforms. Existing systems rely on keyword filters — BHK count, price range, locality dropdown. A buyer who types _"peaceful top-floor flat near a metro with good sunlight"_ gets zero results, or worse, irrelevant ones ranked by keyword frequency.

The root cause: structured property data (101 typed fields per listing) lives in a relational database that has no concept of semantic meaning. Bridging that gap requires:

1. Converting structured fields into a natural-language representation that captures what a buyer would actually say.
2. Embedding that representation into a dense vector space where semantic similarity is a measurable distance.
3. Doing this across a polyglot TypeScript/Python boundary, asynchronously, at throughput, without losing a single event if any service goes down mid-flight.

This pipeline solves all three.

---

## System Architecture

The system runs as a fully containerized polyglot pipeline orchestrated by Docker Compose. Each service owns its data store and communicates via Redpanda (Kafka-compatible) events. The search path is synchronous via gRPC; the ingestion path is fully async.

```
  HTTP Client
      │
      ▼
┌──────────────────────────────────────────┐
│  Node.js / Fastify Gateway               │  REST API · Zod validation
│  (TypeScript, Drizzle ORM)               │  100 RPM rate limiting · Prometheus
└──────────────┬───────────────────────────┘
               │  Single ACID transaction
               ▼
┌──────────────────────────────────────────┐
│  PostgreSQL  (pgvector + HNSW)           │  real_estate_listings  (source of truth)
│                                          │  event_relay_queue     (transactional outbox)
└──────────────┬───────────────────────────┘
               │  SELECT FOR UPDATE SKIP LOCKED · batch 50 · every 2s
               ▼
┌──────────────────────────────────────────┐
│  Event Relay Poller                      │  Background worker inside Gateway process.
│  (Transactional Outbox Worker)           │  JSON row → Protobuf binary → Kafka publish
│                                          │  → row deleted on success.
└──────────────┬───────────────────────────┘
               │  Protobuf binary  [real_estate.listing.ingested]
               ▼
┌──────────────────────────────────────────┐
│  Redpanda  (Kafka-compatible, C++)       │  3 partitions · idempotent producers
│                                          │  real_estate.listing.embedded
│                                          │  real_estate.listing.dlq
└──────────────┬───────────────────────────┘
               │  confluent-kafka consumer
               ▼
┌──────────────────────────────────────────┐
│  Python Intelligence Engine              │  Deserializes Protobuf event.
│  (SentenceTransformers + gRPC)           │  Builds Intelligence Context string.
│                                          │  Generates 384-dim vector (BAAI/bge).
│                                          │  Publishes ListingEmbeddedEvent → Kafka.
│                                          │  Serves gRPC on :50051 for query vectors.
└──────────────┬───────────────────────────┘
               │  Protobuf binary  [real_estate.listing.embedded]
               ▼
┌──────────────────────────────────────────┐
│  Embedding Sync Consumer                 │  Runs inside Gateway process.
│  (@confluentinc/kafka-javascript)        │  Writes vector + intelligence_context
│                                          │  to PostgreSQL via UPDATE.
└──────────────────────────────────────────┘

Search path — synchronous, fires on every GET /search:

  Client → Gateway → ConnectRPC/gRPC → Python gRPC server (vectorizes query text)
                   ← 384-dim float[]
  Gateway runs cosine search against pgvector HNSW index (distance < 0.5)
  Client ← ranked JSON results ordered by similarity score
```

---

## Ingestion Lifecycle

The pipeline is split into two paths: an async ingest path that returns immediately, and a sync search path that blocks only as long as the gRPC call takes.

### Ingest Path (async, eventual consistency)

1. **Ingestion:** A property payload hits `POST /ingest`.
2. **Validation:** Fastify runs the auto-generated Zod schema (derived from the proto) — invalid fields are rejected `400` before touching the database.
3. **ACID Transaction:** Drizzle ORM writes the listing row and a relay event row in a single `BEGIN/COMMIT`. If either fails, both roll back. The client gets `201 Created`.
4. **Outbox Poller:** A background worker fires every 2s, locks up to 50 relay rows with `SELECT FOR UPDATE SKIP LOCKED`, encodes each as Protobuf binary, publishes to Redpanda, then deletes the rows — all inside the same transaction.
5. **Python Consumer:** The Intelligence Engine deserializes the Protobuf event, runs the deterministic context builder to produce a natural-language paragraph from the listing's fields, passes it to `BAAI/bge-small-en-v1.5`, and gets back a 384-dimensional float array.
6. **Vector Sync:** Python publishes a `ListingEmbeddedEvent` back to Kafka. The Node.js embedding sync consumer writes the vector and context string to the listing's row in PostgreSQL. The listing is now semantically searchable.

### Search Path (synchronous)

1. A `GET /search?q=...` request hits the Gateway.
2. The Gateway calls the Python gRPC server over ConnectRPC with the raw query text.
3. Python embeds the query using the same `BAAI/bge-small-en-v1.5` model, returns a 384-dim vector.
4. The Gateway runs `cosineDistance` against the HNSW index in pgvector, filters to distance < 0.5, orders by similarity, paginates.
5. Results return to the client as JSON.

---

## Tech Stack & Engineering Decisions

| Technology                             | Role                       | Rationale                                                                                                                                                                                                                                                             |
| :------------------------------------- | :------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Fastify (Node.js 22)**               | Ingestion Gateway          | Radix-tree router, non-blocking I/O, schema-based serialization — ideal for a write-heavy ingestion path where throughput matters more than business logic.                                                                                                           |
| **Python 3.12 + SentenceTransformers** | Intelligence Engine        | Python has a monopoly on the ML ecosystem. `BAAI/bge-small-en-v1.5` is retrieval-optimized (not a chat model), runs CPU-only at ~157 ms inference, and outputs 384-dim vectors — smaller storage, faster cosine math, no GPU required.                                |
| **Redpanda (Kafka-compatible, C++)**   | Event Bus                  | Single binary, no Zookeeper/KRaft, thread-per-core architecture (Seastar framework) — eliminates JVM GC pauses and external orchestration overhead entirely.                                                                                                          |
| **Protobuf 3 + Buf CLI**               | Cross-language schema      | JSON across a TypeScript/Python boundary fails silently — float precision differs, field renames don't error, no compile-time enforcement. A single `.proto` file compiles to TypeScript types, Zod schemas, Drizzle schemas, and Python classes in one `make proto`. |
| **pgvector + HNSW**                    | Vector search              | Co-locates relational data and vectors in the same row — no sync job, no separate vector DB, ACID guarantees hold for both. HNSW index brings cosine search from O(N) to ~O(log N).                                                                                   |
| **Drizzle ORM**                        | Node.js SQL builder        | Zero-overhead, compiles to raw SQL. Unlike Prisma, no Rust binary engine sits between Node.js and Postgres. Auto-generated from proto via `build-drizzle-schema.ts`.                                                                                                  |
| **Zod 4**                              | API validation             | Auto-generated from proto descriptors including `min`/`max` bounds. Fastify's type provider binds Zod schemas to TypeScript inference — one source of truth for both runtime validation and compile-time types.                                                       |
| **`uv` (Rust-based pip replacement)**  | Python packaging           | Standard `pip install torch sentence-transformers` takes 8–15 minutes in Docker. `uv` with a lockfile drops this to ~45 seconds.                                                                                                                                      |
| **gRPC (grpclib + ConnectRPC)**        | Search query vectorization | Binary protocol, strongly typed via Protobuf, lower overhead than HTTP/REST for the synchronous hot path on every search request.                                                                                                                                     |

---

## Concurrency & Data Integrity

### The dual-write problem

Writing to PostgreSQL and publishing to Kafka are two separate I/O operations with no shared transaction boundary. The naive pattern — insert row, then publish — creates a window where the row commits but the Kafka publish fails (network drop, broker restart). The event is silently lost. The reverse is equally broken.

**Solution — Transactional Outbox:** the ingest handler atomically writes the listing row _and_ a relay event row in a single `BEGIN/COMMIT`. Nothing reaches Kafka until both are durable. The outbox poller then publishes and deletes in a separate transaction — if the publish fails, the row stays, and the poller retries on the next cycle.

### Concurrent poller workers (`SELECT FOR UPDATE SKIP LOCKED`)

Multiple Gateway replicas running the outbox poller simultaneously would process the same relay row multiple times — each replica sees the same `SELECT` result. `SELECT FOR UPDATE SKIP LOCKED` solves this at the database layer: locking a row makes it invisible to every other worker. No Redis, no distributed lock, no coordination service needed. Horizontal scaling is free.

```sql
SELECT * FROM event_relay_queue
ORDER BY created_at ASC
LIMIT 50
FOR UPDATE SKIP LOCKED
```

### Idempotent Kafka producers + manual offset commit

Both the Node.js outbox poller and the Python engine run with `enable.idempotence: true`. The Python consumer commits offsets manually — only after successfully publishing the downstream `ListingEmbeddedEvent`. If the engine crashes between embedding and publishing, it re-processes on restart; the idempotent producer deduplicates the downstream publish. At-least-once delivery with deduplication, end to end.

### Partition keys for strict message ordering

All Kafka messages are keyed on `listing_id`. Kafka's partition router hashes the key — all events for the same listing go to the same partition. Partitions are strictly ordered queues. An update event for a listing cannot be processed before its ingest event, even under 50 concurrent VUs.

---

## Performance & Load Testing

To prove resilience, the system was subjected to a 10,000-event stress spike — 50 concurrent Virtual Users with zero artificial delay, 80% ingest / 20% search (`Math.random() < 0.8` probabilistic split matching real-world read/write ratio).

### The Rate Limiter Problem

The Gateway enforces 100 RPM per IP. Running k6 with 50 VUs at full speed would immediately saturate this, returning `429` before any real pipeline pressure was applied.

**Solution — Dynamic IP Spoofing:** k6 generates a cryptographically random `X-Forwarded-For` header on every request. Fastify's `trustProxy: true` in `NODE_ENV=development` treats each as a distinct user. The rate limiter is bypassed without being disabled — security stays intact, and the deeper pipeline absorbs real load.

```javascript
// load_test.js
function getRandomIP() {
  return `${rand(255)}.${rand(255)}.${rand(255)}.${rand(255)}`;
}
headers: { 'X-Forwarded-For': getRandomIP() }
```

### The Ghost Offset Problem

Between test runs, `kafka-topics.sh --delete` asynchronously flags topics for deletion but leaves `__consumer_offsets` intact. When k6 fired a fresh batch of 8,000 messages, the Python consumer reconnected, found its old committed offset position (`7966`), and demanded that offset on a now-empty partition — stalling indefinitely with a permanent consumer lag.

**Solution:** `run_load_test.sh` executes `docker volume rm` on all Kafka data mounts before every run. This wipes `__consumer_offsets` entirely. Every load test starts from a mathematically clean zero-state.

### Results

| Metric             | Value           |
| :----------------- | :-------------- |
| **Total Requests** | 10,000          |
| **Success Rate**   | 100% (zero 5xx) |
| **Throughput**     | 61.9 req/s      |
| **P50 Latency**    | 540 ms          |
| **P90 Latency**    | 1.41 s          |
| **P95 Latency**    | 1.87 s          |
| **Max Latency**    | 13.46 s         |

### Pipeline Integrity (post-run audit)

| Stage                             | Records |
| :-------------------------------- | :------ |
| PostgreSQL — ingested             | 8,050   |
| Kafka — published                 | 8,050   |
| Python Engine — vectors generated | 8,050   |
| PostgreSQL — embeddings synced    | 8,050   |
| **Pipeline leakage**              | **0**   |
| **DLQ rejections**                | **0**   |

8,050 of 10,000 requests were ingest calls that cleared validation (2,000 were search requests, ~50 were rate-limited or Zod-rejected during warm-up). Perfect 1:1:1:1 parity across all four pipeline stages.

### Service Telemetry (Prometheus + Redpanda Console)

| Service           | Metric                   | Value   | Interpretation                                                      |
| :---------------- | :----------------------- | :------ | :------------------------------------------------------------------ |
| **Fastify**       | Event loop lag (max)     | 11.5 ms | Zod, Protobuf encoding, and Postgres writes are fully non-blocking  |
| **Fastify**       | V8 heap used (peak)      | 45.5 MB | Negligible memory footprint — no tensor allocations in Node.js      |
| **Fastify**       | CPU user time            | 67.9 s  | Low kernel overhead; efficient I/O context switching                |
| **Python Engine** | Avg vector generation    | 157 ms  | CPU-bound ML bottleneck; fully hidden behind Kafka async decoupling |
| **Python Engine** | RSS memory (peak)        | 245 MB  | PyTorch model + SentenceTransformer weights resident in RAM         |
| **Python Engine** | GC uncollectable objects | 0       | Zero tensor leaks across 8,050 consecutive embeddings               |
| **Redpanda**      | Consumer lag (end)       | 0       | Complete pipeline drain confirmed                                   |
| **Redpanda**      | Kafka deadlocks          | 0       | Idempotent producer + manual offset commit working correctly        |

### On P95 = 1.87s

P95 is not a performance problem — it's the expected signature of a mixed workload. The test sends 80% ingest (fast: DB write + outbox insert, non-blocking, P50 ~540ms) and 20% search (slower: synchronous gRPC call to the Python engine). Under 50 concurrent VUs, the Python engine is simultaneously running CPU-bound embedding work for the Kafka consumer in the same process. Search requests that arrive during a heavy embedding burst wait for CPU time and the Python GIL. P95 blowing out to 1.87s is the direct, predictable consequence of running two workloads with different scaling profiles in one process. The roadmap item that splits the Python engine into `embedding-worker` and `query-vectorizer` resolves this entirely.

### GC Uncollectable = 0

`SentenceTransformers` loads a ~150 MB PyTorch model into RAM. If the Kafka consumer loop inadvertently appended tensors to a global list instead of releasing them after each embedding, the Python process would OOM and crash within seconds at this volume. Zero uncollectable objects across 8,050 consecutive embeddings confirms clean tensor lifecycle management throughout.

---

## Design Tradeoffs

**1. Polyglot complexity vs. specialized performance**
Running Node.js and Python together increases operational overhead. The payoff is architectural correctness: Node.js excels at non-blocking I/O ingestion (event loop lag maxed at 11.5ms under full load), while Python has a monopoly on the ML ecosystem for embedding. Forcing the embedding workload into Node.js would mean running a Python subprocess or calling an external API — worse on both latency and reliability.

**2. Async ingest vs. synchronous embedding**
An earlier version had the Gateway call the Python engine over gRPC synchronously during ingest. If the Python service slowed under load, Fastify connections queued behind it. If the engine went down, ingest crashed with it. The pivot to async Kafka decouples both latency and failure — the Gateway returns `201 Created` immediately after the DB write, and the Python engine's 157 ms/vector bottleneck is invisible to the ingest path.

**3. Outbox pattern vs. Debezium CDC**
A full Debezium/Kafka Connect Change Data Capture setup would solve the dual-write problem more cleanly but adds significant infrastructure weight — a Kafka Connect cluster, a Debezium connector, and connector configuration management. The Transactional Outbox with `SELECT FOR UPDATE SKIP LOCKED` achieves the same delivery guarantee with a single extra database table and a background poller. At this scale it's the right tradeoff; at hyperscale Debezium becomes correct.

**4. pgvector vs. dedicated vector database**
Pinecone, Milvus, and Qdrant all require synchronizing data between a relational DB (for structured fields) and a vector DB (for embeddings). Deleting a listing would require two deletes — with an eventual consistency window between them. `pgvector` keeps relational data and vectors in the same row, same ACID transaction. At 10M+ rows the performance ceiling of pgvector becomes relevant; at current scale it's strictly the right choice.

**5. Deterministic context builder vs. LLM synthesis**
The original design used a local Ollama LLM to synthesize property JSON into natural language before embedding. ~5,000 ms per listing, non-deterministic output, and hallucinated amenities that weren't in the source data. Replaced with a deterministic proto-template builder: ~0.5 ms per listing, factually guaranteed by construction, and fully reproducible — re-running the same listing always produces the same context string.

**6. BAAI/bge-small-en-v1.5 vs. OpenAI Ada**
OpenAI's `text-embedding-ada-002` introduces network latency, per-call cost, data privacy exposure, and a hard external dependency. `BAAI/bge-small-en-v1.5` is baked into the Docker image at build time (`HF_HUB_OFFLINE=1` blocks runtime HuggingFace calls), runs CPU-only, and outputs 384 dimensions vs. Ada's 1536 — smaller pgvector storage, faster cosine distance computation, no cold start.

---

## Infrastructure Hardening

After the initial build, the following vulnerabilities were audited and resolved before load testing:

- **Ghost Offset Eradication:** `kafka-topics.sh --delete` leaves `__consumer_offsets` intact. Stale offsets from a previous test run caused the Python consumer to stall permanently on the next run (demanding offset `7966` on an empty partition). Fixed via `docker volume rm` on Kafka mounts in `run_load_test.sh` — every run starts from verified zero-state.

- **Protobuf Buffer Encoding Mismatch:** `@confluentinc/kafka-javascript` encodes message values as raw `Buffer`, but the initial Python consumer read them as hex-encoded strings, corrupting the byte stream. Fixed by aligning both sides on raw binary: `Buffer.from(toBinary(...))` on the Node.js side, `from_binary()` on the Python side.

- **`betterproto` → `protobuf-py` Migration:** The Python engine originally used `betterproto` for Protobuf deserialization. After the schema was refactored to vertical slices, `betterproto`'s custom extension import path resolution broke. Migrated to `protobuf-py`, which handles custom `info`/`validation` extensions correctly.

- **Graceful Shutdown on `SIGTERM`:** Without explicit signal handling, Docker killing the Gateway mid-cycle would lose any relay events currently in-flight through the outbox poller. Bound `SIGTERM`/`SIGINT` to an `AbortSignal` that stops the poller after the current batch finishes, drains Kafka producer connections, and releases PostgreSQL connections before exiting.

- **Fail-Fast Environment Validation:** Standard Node.js apps boot successfully with missing environment variables and crash only when the missing value is first accessed. Fastify now parses `process.env` through a Zod schema at boot — if `KAFKA_BROKER_URL` or `DATABASE_URL` is missing or malformed, the container exits immediately with a detailed schema error before binding to port 3000.

- **Dead Letter Exchange (DLX):** Any message that fails Protobuf deserialization or business-logic validation in either consumer is routed to `real_estate.listing.dlq` with `error`, `original_topic`, and `original_partition` as Kafka headers. The offset is committed unconditionally — the partition keeps moving, no `CrashLoopBackOff`. A standalone `npm run dlq:replay` script re-publishes DLQ messages to their original topic with `replayed: true` and `replayed_at` headers.

- **Deep Metrics without the Bloat:** The Python `prometheus_client` exports `process_cpu_seconds_total` (unified), not the split labels the Node.js client exports. Standard Kafka monitoring requires a Java `kafka-exporter` container. Fixed by a custom Bash `fetch_prom` function with `jq` fallback logic for differing CPU label schemas, and direct Redpanda Console REST API scraping via inline Python one-liners — 40+ metrics, zero extra containers.

---

## Observability

The project includes a fully configured Prometheus stack scraping both services every 5 seconds.

| Target            | Exporter                | Metrics                                                                                                                                                 |
| :---------------- | :---------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Fastify**       | `fastify-metrics`       | HTTP request durations (histogram), event loop lag (P50/P99/max), V8 heap used/total, GC pause duration, active handles, open FDs, CPU user/system time |
| **Python Engine** | `prometheus-client`     | `engine_embeddings_total` (counter), `engine_embedding_processing_seconds` (summary), GC collections/uncollectable objects, RSS memory, CPU time        |
| **Redpanda**      | Console REST API (Bash) | Broker count, partition count, consumer group lag, partition offsets, active members, dead groups — no Java `kafka-exporter` required                   |

`generate_metrics_md.sh` pulls all 40+ metrics from Prometheus and Redpanda Console after every load test run and renders a complete `metrics.md` artifact automatically.

---

## Engineering Journey

**Phase 1 — Foundation:** Scaffolded Fastify + Drizzle + Postgres. Targeted PostgreSQL 18 for native `uuidv7()` — PG18's Docker volume layout had changed, mounts failed. Fell back to PG16 + custom SQL function.

**Phase 2 — The Protobuf Battle:** Started with JSON on Kafka. Pivoted to Protobuf for schema enforcement across the TypeScript/Python boundary. `@confluentinc/kafka-javascript` encoded message values differently than the Python consumer expected — hex vs. raw binary. Debugged the buffer mismatch. Wrote the codegen scripts to auto-generate Zod and Drizzle schemas from proto descriptors. Migrated from `betterproto` to `protobuf-py` when extension imports broke after schema refactoring.

**Phase 3 — The LLM Pivot:** Original design: Ollama LLM to generate natural language from property JSON before embedding. ~5,000 ms per listing, hallucinated amenities, non-deterministic. Replaced with the deterministic proto-template context builder. Processing dropped to ~0.5 ms per listing.

**Phase 4 — The Search Pivot:** Original design: synchronous gRPC call to Python engine during ingest. Under load, the ML bottleneck propagated directly to ingest latency. Replaced with async Kafka for the ingest path — Gateway returns `201` immediately. gRPC kept only for the search query vectorization path where synchronous response is unavoidable.

**Phase 5 — Hardening:** Kafka consumer race condition fix. DLQ architecture. HNSW index. Rate limiting. Graceful shutdown with `AbortSignal`. DLQ replay tooling.

**Phase 6 — Observability Chaos:** k6 failed with `429` — engineered the IP-spoofing bypass. k6 then failed with `400` — the script asserted `200 OK` but the API correctly returns `201 Created`. Fixed assertions. PromQL `{job="engine"}` broke in Bash due to double-quote expansion. Built the full `generate_metrics_md.sh` pipeline parsing 40+ JSON payloads into a rendered artifact.

```
f9e534f  chore: init infra and scaffold project
6d1559b  chore: integrate local ollama infra for text synthesis
640a657  chore: upgrade postgresql to v18 for native uuidv7 support
f904cbf  fix: create extension vector on first run and uuidv7 function name fix
7154f08  fix: pg18 volume requires new layout, remove volume and restart container
bd7a66f  feat: implement core gateway and kafka and outbox poller
758f732  feat: add EmbeddedPropertyEvent schema for ML vector pipeline
70dba16  feat: scaffold and complete python stateless engine
3803398  refactor: add natural tone/lang in proto as metadata, change betterproto → protobuf-py
c4c6fcc  refactor: replace llm context builder with deterministic protobuf metadata
fbe2d78  refactor: change proto schemas to adapt to events, attributes and rpc calls
3b2dd63  refactor: introduce makefile and refactor protobuf toolchain
0a51690  feat: automate zod and drizzle schema generation
5b381a4  refactor: api handlers and outbox poller with validation end-to-end
85425ba  fix: change varchar → text in postgres for no max length and no perf loss
9374945  feat: implement embedding consumer to persist property vector
b36cdb6  feat: implement async gRPC vector search server
c347450  feat: implement semantic search with connectrpc and pgvector
b87b4bc  fix: resolve end-2-end semantic search pipeline and grpc serialization
ac24faf  fix: mitigate kafka consumer race condition and metadata caching
c027f4c  feat: build dlq architecture to consume errored messages
b4b2f53  feat: implement HNSW vector indexing for embedding column
22db3a1  feat: enforce rate limiting and payload protection
860ddf6  feat: implement graceful shutdown for transactional outbox
68658c8  feat: implement standalone DLQ replay script
ee62058  feat: integrate end-to-end performance testing and prometheus metrics suite
ca76d75  feat: automate load test performance report generation
```

---

## Project Structure

```
.
├── shared/
│   └── proto/                              # Single source of truth — drives everything downstream
│       ├── real-estate/
│       │   ├── real-estate-listing.proto   # 101 fields, custom info{template, searchable} + validation{min, max}
│       │   ├── listing-events.proto        # ListingIngestedEvent, ListingEmbeddedEvent
│       │   ├── enums.proto                 # PropertyType, FacingDirection, FurnishingStatus, etc.
│       │   └── metadata.proto              # Custom proto extensions: info, validation
│       └── semantic-search/
│           └── semantic-search.proto       # SemanticSearchEngineService gRPC definition
│
├── services/
│   ├── gateway/
│   │   └── src/
│   │       ├── api/
│   │       │   ├── ingest/                 # POST /ingest — controller + route
│   │       │   └── search/                 # GET /search — controller, route, ConnectRPC client
│   │       ├── db/                         # Drizzle client, schema.ts (generated schema + eventRelayQueue)
│   │       ├── messaging/
│   │       │   ├── kafka.client.ts         # Kafka setup, idempotent producer, retry-on-connect
│   │       │   ├── consumers/
│   │       │   │   └── embedding-sync.consumer.ts   # embedded events → UPDATE vector in Postgres
│   │       │   └── workers/
│   │       │       └── event-relay.worker.ts        # SKIP LOCKED → Protobuf → Kafka → delete
│   │       ├── config/                     # env.ts (Zod-validated at boot), constants.ts
│   │       ├── scripts/
│   │       │   ├── build-drizzle-schema.ts # Proto descriptors → Drizzle/Postgres table schema (codegen)
│   │       │   ├── build-zod-schema.ts     # Proto descriptors → Zod validators (codegen)
│   │       │   └── replay-dlq.ts           # Standalone DLQ replay tool
│   │       ├── gen/                        # Auto-generated — do not edit
│   │       │   ├── drizzle/                # real-estate-drizzle-schema.ts
│   │       │   ├── zod/                    # real-estate-zod-schema.ts
│   │       │   └── real-estate/            # TypeScript Protobuf types (buf)
│   │       └── server.ts                   # Fastify bootstrap, plugins, graceful shutdown
│   │
│   └── engine/
│       └── src/
│           ├── ml/
│           │   ├── intelligence_context_generator.py   # Proto templates → natural language paragraph
│           │   └── semantic_vectorizer.py              # SentenceTransformer.encode()
│           ├── messaging/
│           │   ├── kafka_client.py                     # confluent-kafka consumer + idempotent producer
│           │   └── consumers/
│           │       └── listing_ingested_consumer.py    # Kafka loop → context → embed → publish
│           ├── rpc/
│           │   └── semantic_search_server.py           # gRPC server: vectorizes search queries
│           ├── config/                                 # pydantic-settings, Kafka topic constants
│           └── gen/                                    # Auto-generated Python Protobuf types (buf)
│
├── db/init/01-pgvector.sql                # CREATE EXTENSION vector; custom uuidv7() SQL function
├── prometheus/prometheus.yml              # Scrapes gateway :3000/metrics + engine :8000 every 5s
├── scripts/
│   ├── load_test.js                       # k6: random IP spoofing, 80/20 split, 201-aware assertions
│   ├── run_load_test.sh                   # Volume wipe → stack restart → k6 → generate_metrics_md
│   └── generate_metrics_md.sh            # 40+ PromQL queries + Redpanda Console API → metrics.md
├── docker-compose.yml
└── Makefile
```

---

## Quick Start

**Prerequisites:** Docker, Docker Compose, Node.js 22+, Python 3.12+, [Buf CLI](https://buf.build/docs/installation) — k6 only needed for load testing.

```bash
# 1. Clone and start the full stack
git clone https://github.com/zed-inn/<repo-name>
cd <repo-name>
make up

# 2. Ingest a listing
curl -X POST http://localhost:3000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "city": "Delhi",
    "state": "Delhi",
    "pinCode": 110001,
    "priceCrore": 2.8,
    "bhk": 3,
    "propertyType": "APARTMENT",
    "distanceMetroKm": 0.8,
    "hasGymnasium": true,
    "hasSwimmingPool": true,
    "isGatedCommunity": true,
    "hasEvCharging": true,
    "furnishingStatus": "FULLY_FURNISHED",
    "walkabilityScore": 8.5
  }'
# → { "success": true, "listing_id": "019xyz-uuidv7..." }

# 3. Wait ~5s for the async embedding pipeline, then search
curl "http://localhost:3000/search?q=peaceful+gated+3BHK+near+metro+with+gym&limit=5&page=1"
```

```bash
make up           # Build and start all services
make down         # Stop all services
make logs         # Tail all container logs
make proto        # Regenerate all code from .proto files
make load-test    # Run the full k6 load test (requires k6)
make dlq-replay   # Replay failed messages from the DLQ
```

---

## API Reference

### `POST /ingest`

**Required:** `city` (string), `state` (string), `pinCode` (6-digit int), `priceCrore` (float)

**Optional:** 97 additional fields — BHK, property type, carpet area, furnishing status, amenities (gym, pool, clubhouse, pooja room, dog park, and 20+ more), security (CCTV, biometric, boom barrier, 24x7 guard), infrastructure (EV charging, solar panels, piped gas, STP, rainwater harvesting), proximity to metro/airport/hospital/school/mall/bus stop, livability scores (walkability, safety, green cover). The intelligence context is built from whatever subset is provided — a listing with 4 fields produces a shorter context than one with 40; both are semantically searchable.

```json
{ "success": true, "listing_id": "019xyz-uuidv7..." }
```

### `GET /search`

| Parameter | Type   | Default  | Constraint   |
| :-------- | :----- | :------- | :----------- |
| `q`       | string | required | min length 2 |
| `limit`   | number | 10       | 1–50         |
| `page`    | number | 1        | ≥ 1          |

Results filtered to cosine distance < 0.5, ordered by descending similarity. Each result includes `id`, `city`, `priceCrore`, `bhk`, `propertyType`, `intelligenceContext`, and `similarityScore`.

### `GET /health` · `GET /metrics`

Health check and Prometheus scrape endpoint (gateway).

---

## Roadmap

**Next — React search UI**
A search input and property results grid with similarity scores and the intelligence context visible per result. Makes the semantic ranking self-evident: you can see _why_ a listing ranked where it did — which is the hardest part to explain to a non-technical audience.

**Near-term — Split Python engine into two services**
One process handles two workloads with different scaling curves: the Kafka embedding worker (CPU-bound, scales with partition count) and the gRPC query vectorizer (latency-sensitive, scales with search QPS). This is the root cause of the P95 tail. Splitting into `embedding-worker` and `query-vectorizer` takes ~1 day and removes the coupling entirely — 3 partitions → 3 embedding replicas → ~19 vectors/s; query-vectorizer scales independently.

**Near-term — Hybrid search: BM25 + vector (RRF)**
Pure vector search underperforms on queries with specific proper nouns — society names, Metro stations, localities — where keyword matching outperforms dense similarity. Adding PostgreSQL `tsvector`/`tsquery` full-text search and merging ranked lists via Reciprocal Rank Fusion (RRF) handles both query types correctly.

**Later — Re-embedding backfill on schema change**
New proto fields produce correct vectors for new listings but leave existing rows with stale embeddings. A backfill job that re-runs the context builder and re-embeds rows where `intelligence_context` doesn't contain the new field's template string is needed before schema evolution is safe at scale.

**Later — Grafana dashboards**
Importable JSON for both services: request throughput + P95 latency (gateway), embedding rate + consumer lag over time (engine). Auto-provisioned via Docker Compose volume mount into the Grafana container.
