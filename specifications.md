# Fraud Detection Engine - Technical Specifications

## 1. Introduction

The Fraud Detection Engine is an autonomous, high-throughput, horizontally scalable decision engine specifically designed for extreme mission-critical transactional environments. Employing state-of-the-art domain-driven design, real-time message brokering, asynchronous execution semantics, and intense cryptographic validations, the platform assesses transaction veracity to pinpoint adversarial velocity clusters, irregular geospatial behavior patterns, and sanction violations within microsecond thresholds. This specifications document rigorously details every core, utility, data store, and rule-orchestration module.

## 2. Core Architecture Overview

The system is rigorously partitioned into highly cohesive and loosely coupled modules utilizing the dependency injection pattern in addition to an intricate event-driven paradigm. This minimizes blocking I/O and maximizes pipeline throughput. The four primary modules consist of:
1.  **Core Domain Models (`src/core`)**: The system's heart, managing strongly typed interfaces, event envelope schemas, security verification paradigms, and the global dependency resolution container.
2.  **Robust Event Infrastructure (`src/events`)**: Kafka-powered ingestion and broadcast ecosystem capable of massive scale processing, complete with explicit retry protocols and dead-letter queues.
3.  **Dynamic Store Management (`src/store`)**: Multi-layered state persistence utilizing Redis `eval` Lua scripts for lightning-fast atomic accumulations and PostgreSQL for persistent, append-only, temporally immutable event logs.
4.  **Extensible Rules Engine (`src/rules`)**: Pluggable heuristic modules (geospatial algorithms, sliding-window velocity aggregators, static merchant blocklists) managed by a central orchestrator which computes global weighted risk scores.

---

## 3. Detailed Component Specifications

### 3.1 Domain Models & Security (`src/core`)

#### 3.1.1 Event Envelopes (`core/domain_models/messaging/event-envelope.schema.ts`)
The `MessageEnvelope` acts as the universal schema for all inter-process communications over Kafka. It encapsulates:
*   **Metadata**: Propagates schema version (`schemaVersion`), chronological precision mapping in nanoseconds (`createdAtNs`), and execution lineage (`provenanceTrace`).
*   **Payload**: The underlying polymorphic business logic payload (e.g., `TransactionInitiated`, `TransactionFlagged`, or `TransactionValidated`).
*   **Signature**: A cryptographically generated HMAC SHA-256 verification string ensuring payload immunity against man-in-the-middle attacks and data tampering.

#### 3.1.2 Cryptographic Validator (`core/domain_models/security/crypto-validator.service.ts`)
A critical component preventing adversarial injections and payload modifications. It implements the singleton paradigm with secure configuration initialization. 
*   **Mechanism**: Uses Node.js native `crypto.createHmac`. 
*   **Integrity Verification**: `verify()` performs constant-time string comparison (`crypto.timingSafeEqual`) to eliminate timing-channel vulnerabilities.

#### 3.1.3 Dependency Injection Configurator (`core/domain_models/dependency_config.ts`)
Manages the instantiation sequence and topological dependency graph of the entire ecosystem. It utilizes Awilix to provision Singletons and Scoped dependencies. Validations ensure cyclical dependencies are aborted gracefully during the application bootstrap sequence.

### 3.2 Utilities Subsystem (`src/utils`)

#### 3.2.1 Metrics Collector (`utils/metrics/metrics-collector.ts`)
Built upon `prom-client`, this module instruments the entire application ecosystem for Prometheus-based scrapes.
*   **Counters**: Tracks application-level ingestion, successful rule evaluations, failure anomalies, and dead-letter queue routing triggers.
*   **Histograms**: Deployed strategically within the orchestrator to track nanosecond precision pipeline latency (`risk_aggregator_duration`, `rule_evaluation_latency`).

#### 3.2.2 Cryptographic Manager (`utils/security/crypto.ts`)
A unified security manager utilizing Advanced Encryption Standard (AES) with GCM mode for symmetric payload encryption and Ed25519 for elliptic curve digital signature derivations. Designed for securing ultra-sensitive `PII` nested within event envelopes prior to database serialization.

### 3.3 Messaging Infrastructure (`src/events`)

#### 3.3.1 Kafka Consumer (`events/client/kafka_client/consumer/consumer.ts`)
An idempotent message ingestion processor bound to the Kafka broker.
*   **Ingestion Logic**: Adheres to `at-least-once` delivery semantics. Batches transactions, parses `BigInt` safely using revivers to counter strict JSON limitations, and invokes cryptographic integrity checks.
*   **DLQ Routing**: Gracefully handles exceptions by routing malformed schemas or poisoned messages to a dedicated Dead Letter Queue topic, ensuring consumer loops never stall.

#### 3.3.2 Kafka Producer (`events/client/kafka_client/producer/producer.ts`)
Asynchronous transmission agent featuring retry backoffs and idempotency flags. Ensures outgoing flagging verdicts (`TransactionFlagged`) are successfully broadcast to downstream mitigation services. Utilizes `JSON.stringify` replacers to format `BigInt` chronologies into string buffers prior to dispatch.

### 3.4 Persistent Store Components (`src/store`)

#### 3.4.1 Postgres Event Repository (`store/event_store/postgres_impl`)
An append-only event source mechanism that establishes the absolute source of truth for the system.
*   **Optimistic Concurrency Control**: Adheres to ACID properties by enforcing snapshot isolation levels. Validates stream versions (`appended_version == latest_version + 1`) to eliminate race conditions.
*   **Integrity Restorations**: Prior to reconstituting aggregate boundaries in-memory, the repository invokes `CryptoValidator` on the database contents. Any signature corruption throws an `IntegrityViolationError`.

#### 3.4.2 Redis Projection Store (`store/projection_store/projection-store.ts`)
The high-velocity, ephemeral cache mapping user consumption profiles. Let's explore its atomic mechanisms:
*   **Lua Execution Protocols (`processTransaction`)**: Leverages `ioredis` parameterized `.eval()` capabilities. The Lua program increments `user:{userId}:balance` and concurrently appends transactions to a sorted set `user:{userId}:transactions`, utilizing Epoch timestamps for the sorting mechanism. Extraneous records exceeding the temporal sliding window bounds are `ZREMRANGEBYSCORE` culled atomically.

### 3.5 Extensible Rules Engine (`src/rules`)

#### 3.5.1 Geospatial Processor (`rules/registry/dynamic_rules/rules/geospatial`)
Calculates geographic divergence and anomalous physical velocities using the deeply mathematical Haversine formula.
*   **Analysis Workflow**: When a transaction activates, the Engine maps current telemetry (Latitude/Longitude). It accesses previous geospatial profiles spanning the last hour from Redis. If the speed needed to travel from Location A to Location B exceeds aeronautical probability (e.g. 5,000 km/h), the engine produces an `impossible_travel_detected` boolean mapping.

#### 3.5.2 Velocity Aggregator (`rules/registry/dynamic_rules/rules/velocity`)
Ascertains high-frequency spending anomalies by observing the `ProjectionStore`.
*   **Detection Parameters**: Implements temporal window queries to uncover scenarios such as:
    1. Submitting 5 separate transactions within 60 seconds (Card testing).
    2. Exploding the aggregate volumetric consumption beyond maximum user limits within an hour.

#### 3.5.3 Rule Registry & Orchestration (`rules/registry/...` & `rules/engine/...`)
The brain center executing dynamically registered rules.
*   **Concurrent Execution (`core/engine.ts`)**: Uses `Promise.allSettled`, dispatching transaction variants dynamically to all configured handlers.
*   **Risk Score Aggregation (`aggregator.ts`)**: Ingests deterministic output probabilities (0 to 1) assigned by individual rules, scaling them by normalized weight factors (`geo = 2.0`, `velocity = 1.0`). If the final outcome overtakes a predefined threshold (`fraud_threshold`), it triggers the `AlertingSubsystem`.
*   **Alert Dispatcher (`alerts.ts`)**: Commits idempotent `SETNX` records to Redis to halt duplicate alerting loops for identical events, preceding final transmission across the primary `fraud-alerts-high-priority` stream.

## 4. Operational Scalability & Disaster Recovery
The implementation mandates horizontally-scaling orchestration pods. The utilization of stateless heuristic computing alongside atomic datastore commands ensures `O(1)` node synchronicity delays.
In occurrences of Postgres connection decay, pooling fail-safes are instantiated. For Kafka cluster degradations, the local client employs buffering alongside `maxInFlightRequests`. For comprehensive operational verification, the `MetricsCollector` maps throughput parameters globally, triggering upstream DevOps notifications through standard Prometheus alerting vectors.
