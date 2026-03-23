-- Event Store Schema DDL
-- This script defines the immutable transaction log for the fraud detection engine.
-- Designed for high throughput and consistent event sourcing.

BEGIN;

-- Enable UUID extension if not already available
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Main events table storing immutable transaction logs.
-- Append-only access policy should be enforced at the database role level.
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id UUID NOT NULL,
    version BIGINT NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    signature TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Optimistic Concurrency Control: Ensures each aggregate has a unique
    -- version sequence, preventing race conditions on updates.
    CONSTRAINT unique_aggregate_version UNIQUE (aggregate_id, version)
);

-- Indexing Strategy:

-- 1. GIN index on 'payload' for efficient querying of JSONB data.
-- This allows fast retrieval based on nested fields within the event payload.
CREATE INDEX IF NOT EXISTS idx_events_payload_gin ON events USING GIN (payload);

-- 2. B-Tree index on (aggregate_id, version) for fast stream loading.
-- This is crucial for reconstructing aggregate state by loading events in
-- their correct sequence.
CREATE INDEX IF NOT EXISTS idx_events_aggregate_sequence ON events (aggregate_id, version);

-- 3. BRIN index on 'created_at' for efficient range scans by time.
-- This helps in audit log retrieval and provides a foundation for future 
-- table partitioning strategies by date.
CREATE INDEX IF NOT EXISTS idx_events_created_at_brin ON events USING BRIN (created_at);

-- Set comment for clarity
COMMENT ON TABLE events IS 'Immutable transaction log for event sourcing';
COMMENT ON COLUMN events.payload IS 'JSONB event data payload';
COMMENT ON COLUMN events.signature IS 'HMAC-SHA256 signature for data integrity';
COMMENT ON COLUMN events.version IS 'Sequential version number per aggregate for optimistic locking';

COMMIT;
