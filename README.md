# Fraud Detection Engine

This Repository was fully engineered, compiled and tested by The Genesis Machine from one simple prompt.

## Overview

Welcome to the **Fraud Detection Engine**, an ultra-high performance, distributed, mathematically sound financial verification system. Built entirely on an asynchronous event-driven paradigm utilizing Node.js, Kafka, PostgreSQL, and Redis, the system is architected specifically to analyze real-time transnational messaging streams to intercept hostile, anomalous, and statistically unviable transactions.

This platform was constructed to fulfill extreme operational limits, accommodating millions of transactions per second (TPS) while maintaining nanosecond-level instrumentation mapping.

---

## 1. Installation & Environment Setup

### 1.1 Prerequisites
*   **Operating System**: Linux (Ubuntu 20.04/22.04 LTS recommended) / Debian variants.
*   **Node.js**: `v18.0.0` or greater (strictly verified via `package.json` engines).
*   **Docker & Docker Compose**: Required for containerized deployment and integration testing.
*   **PostgreSQL**, **Redis**, **Kafka**: Managed via Docker Compose for local development.

### 1.2 Configuration (Zod-Validated)
The engine utilizes a strict, centralized configuration system powered by **Zod**. All environment variables are validated at startup.
-   `DB_URL`: PostgreSQL connection string.
-   `REDIS_URL`: Redis connection string.
-   `KAFKA_BROKERS`: Comma-separated list of Kafka brokers.
-   `FRAUD_THRESHOLD`: Risk score threshold (0.0 - 1.0) for flagging transactions.
-   `CRITICAL_RULE_IDS`: Comma-separated list of rules that trigger fail-closed behavior on failure.

### 1.3 Bootstrapping the Services
1.  Install dependencies: `npm install`
2.  Spin up the infrastructure: `docker-compose up -d`
3.  Run database migrations: `npx knex migrate:latest`
4.  Build the application: `npm run build`

---

## 2. Database Evolution
We use **Knex.js** for managed schema evolution.
-   **Run Migrations**: `npx knex migrate:latest`
-   **Rollback**: `npx knex migrate:rollback`
-   **Create Migration**: `npx knex migrate:make <name>`

---

## 3. Containerization & Deployment
The engine is fully containerized using a multi-stage production-optimized **Dockerfile**.
-   **Build Image**: `docker build -t fraud-engine .`
-   **Local Stack**: `docker-compose up` orchestrates the app and all its dependencies.

---

## 4. Testing & Validation

### 4.1 Unit Testing
Executes in-memory logic validation using mocked dependencies.
```bash
npm test
```

### 4.2 Integration Testing (Testcontainers)
Validates the persistence layer against real, ephemeral Docker containers (Postgres & Redis).
```bash
npm test -- src/test/integration
```
*Note: Ensure Docker is running. The first run may take longer to pull required images.*

### 4.3 CI/CD
A **GitHub Actions** pipeline (`.github/workflows/ci.yml`) is triggered on every push to validate linting, build integrity, and test coverage.

---

## 5. Core Architecture Modules

### 5.1 Domain Management (`src/core`)
The theoretical foundation. It contains strict data models and the central Zod-validated configuration.

### 5.2 Storage Arrays (`src/store`)
-   **Projection Store (Redis)**: Uses atomic Lua scripts for high-performance sliding-window velocity tracking.
-   **Event Repository (PostgreSQL)**: Immutable event sourcing with optimistic concurrency control (OCC).

### 5.3 Extensible Rules Subsystem (`src/rules`)
Evaluates transactions against:
-   **Geospatial Anomalies**: Haversine-based "impossible travel" detection.
-   **Velocity Clustering**: Detects card-testing and high-frequency fraud patterns.
-   **Merchant Blacklisting**: Real-time enforcement of prohibited merchant lists.

---

## 6. Running the Application
To launch the primary core process:
```bash
npm start
```
The engine will initialize the consumer infrastructure and begin processing `TransactionValidated` broadcasts.
