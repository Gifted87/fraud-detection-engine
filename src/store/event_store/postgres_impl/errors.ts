/**
 * Base class for all Event Store errors.
 * Provides structured error context and standardized serialization for observability.
 */
export abstract class EventStoreError extends Error {
  public readonly timestamp: number;

  constructor(
    public readonly code: string,
    public readonly message: string,
    public readonly context: Record<string, unknown>,
    public readonly retryable: boolean
  ) {
    super(message);
    this.name = 'EventStoreError';
    this.timestamp = Date.now();
    Error.captureStackTrace(this, this.constructor);
  }

  public toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      context: this.context,
      retryable: this.retryable,
      timestamp: this.timestamp,
      stack: this.stack,
    };
  }
}

/**
 * Triggered when a version constraint (Optimistic Concurrency) fails in the database.
 */
export class OptimisticConcurrencyError extends EventStoreError {
  constructor(
    aggregateId: string,
    expectedVersion?: bigint,
    actualVersion?: bigint
  ) {
    const msg = expectedVersion !== undefined && actualVersion !== undefined
      ? `Optimistic concurrency conflict for aggregate ${aggregateId}: expected version ${expectedVersion}, but found ${actualVersion}.`
      : `Optimistic concurrency conflict for aggregate ${aggregateId}.`;
    super(
      'ERR_CONCURRENCY_CONFLICT',
      msg,
      { 
        aggregateId, 
        expectedVersion: expectedVersion?.toString() ?? 'unknown', 
        actualVersion: actualVersion?.toString() ?? 'unknown' 
      },
      true
    );
    this.name = 'OptimisticConcurrencyError';
  }
}

/**
 * Triggered when event payloads cannot be parsed or transformed for storage.
 */
export class SerializationError extends EventStoreError {
  constructor(eventType: string, details: string) {
    super(
      'ERR_SERIALIZATION_FAILED',
      `Failed to serialize event of type ${eventType}.`,
      { eventType, details },
      false
    );
    this.name = 'SerializationError';
  }
}

/**
 * Triggered when cryptographic verification fails or a hash mismatch is detected.
 * This is a security-critical violation.
 */
export class IntegrityViolationError extends EventStoreError {
  constructor(aggregateId: string, eventId: string, reason: string) {
    super(
      'ERR_INTEGRITY_VIOLATION',
      `Integrity violation detected for aggregate ${aggregateId}, event ${eventId}: ${reason}`,
      { aggregateId, eventId, reason },
      false
    );
    this.name = 'IntegrityViolationError';
  }
}

/**
 * Triggered by database connectivity issues, connection timeouts, or pool depletion.
 */
export class ConnectionError extends EventStoreError {
  constructor(reason: string, poolStats?: Record<string, unknown>) {
    super(
      'ERR_CONNECTION_FAILURE',
      `Database connection failure: ${reason}`,
      { reason, poolStats: poolStats || {} },
      true
    );
    this.name = 'ConnectionError';
  }
}
