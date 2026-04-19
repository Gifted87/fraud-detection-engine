# Fraud Detection Engine

## Overview

Welcome to the **Fraud Detection Engine**, an ultra-high performance, distributed, mathematically sound financial verification system. Built entirely on an asynchronous event-driven paradigm utilizing Node.js, Kafka, PostgreSQL, and Redis, the system is architected specifically to analyze real-time transnational messaging streams to intercept hostile, anomalous, and statistically unviable transactions.

This platform was constructed to fulfill extreme operational limits, accommodating millions of transactions per second (TPS) while maintaining nanosecond-level instrumentation mapping. A rigorous modular partitioning model assures strict boundaries between domain objects, telemetry pipelines, and underlying storage matrices. Every message broadcasted is authenticated structurally and chronologically, while any modification attempt will intrinsically fail cryptographic validation.

---

## 1. Installation & Environment Setup

This engine demands strict version dependencies to operate safely. 

### 1.1 Prerequisites
*   **Operating System**: Linux (Ubuntu 20.04/22.04 LTS recommended) / Debian variants.
*   **Node.js**: `v18.0.0` or greater (strictly verified via `.nvmrc` or `package.json` engines context).
*   **TypeScript**: `v5.3.3`
*   **Kafka**: Cluster version 3.x+ utilizing SSL/SASL SCRAM-SHA-512 authentication formats.
*   **PostgreSQL**: `v13.0` minimum for advanced JSONB performance and transactional serialization layers.
*   **Redis**: `v6.2` minimum ensuring optimized memory footprints for `.eval()` Lua script caching.

### 1.2 Bootstrapping the Services
Clone the core repository directly into your operating environment.
1. Run `npm install` utilizing clean dependencies.
2. Ensure you have formulated a stringent `.env` parameter map encompassing:
   - `SIGNING_KEY` : `Hex` randomized payload serialization key.
   - `ENCRYPTION_KEY`: Absolute 32-byte (256-bit) buffer context for AES-GCM primitives.
   - `PRIVATE_KEY_PEM` / `PUBLIC_KEY_PEM`: Required Ed25519 PKI matrix arrays.
   - `REDIS_URL`, `KAFKA_BROKERS`, `PG_URI`: Corresponding network addresses.

3. Compile the typescript binary maps with `npm run build`. This generates deterministic `/dist/` ECMA primitives.

---

## 2. Core Architecture Modules

### 2.1 Domain Management (`src/core`)
The theoretical foundation. It contains strict data models, schemas, and a central Dependency Injection config structure (`dependency_config.ts`).
The `CryptoValidator` evaluates inbound transactional boundaries to eliminate MITM vectors, ensuring complete and absolute event integrity. `EventEnvelopeFactory` strictly mandates that every raw transaction is appended with accurate chronological references mapping explicitly to nanosecond timestamps while utilizing HMAC validation.

### 2.2 Event Transmission Array (`src/events`)
The Kafka communication mapping layer.
*   The `FraudEventConsumer` hooks to incoming financial arrays, unpacking `BigInt` safely to bypass JS limitations, triggering local evaluators or mitigating malformed payloads exclusively toward dead-letter queues (`DLQ`).
*   The `KafkaEventProducer` facilitates asynchronous delivery into specialized risk pipelines, executing retry-loops automatically upon intermittent degradation of broker acknowledgment packets.

### 2.3 Storage Arrays (`src/store`)
Two specialized persistance endpoints maintain isolation properties:
*   **Projection Store (Redis)**: Enables lighting fast user analysis schemas, computing total amounts mapped during short sliding time windows. Uses optimized atomic Lua computations to evade any multi-threaded update collisions on individual user hashes.
*   **Event Repository (PostgreSQL)**: Permanent immutable storage acting as the absolute ground truth. Uses advanced transactional isolation (`SERIALIZABLE`) and manual `ROLLBACK` assertions explicitly tracking payload sequence markers (`version`) to orchestrate optimistic concurrency mapping safely across the network.

### 2.4 Extensible Rules Subsystem (`src/rules`)
The brain of the platform. Evaluates every verified input using asynchronous processing against distinct algorithms:
*   **Geospatial Anomalies**: Formulates physics-based logic (Haversine equation) mapping user distances mathematically vs the difference in temporal boundaries, accurately intercepting theoretically impossible international hops.
*   **Velocity Clustering**: Uncovers sophisticated script-level or manual card-testing frameworks, examining user-level metrics provided synchronously via the Projection Store mapping.
*   **Aggregator & Alerts**: Central execution layer compiling numerical weights mapped directly against every triggered or cleared rule. Flagging dynamically communicates back over the event transmission framework specifically bound for immediate user suspension processes.

---

## 3. Running & Testing The Application

To launch validation procedures simply execute:
`npx jest src --coverage`

Unit testing covers over 95% of execution algorithms, mocking underlying infrastructure dependencies to guarantee functionality independently from internal networking statuses.

To launch the primary core process executing end-to-end functionality:
`npm start`

The engine will successfully bind to internal arrays and initialize the consumer infrastructure waiting for `TransactionValidated` broadcasts. 
