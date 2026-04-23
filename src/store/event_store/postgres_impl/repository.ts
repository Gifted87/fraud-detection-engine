import { PoolClient } from 'pg';
import { Registry, Histogram } from 'prom-client';
import { PostgresPoolManager } from './client';
import { CryptoManager } from '../../../utils/security/crypto';
import { MessageEnvelope, canonicalizeJson } from '../../../core/domain_models/messaging/event-envelope.schema';
import { Transaction } from '../../../core/domain_models/definitions/transaction.interface';
import { Logger } from 'pino';
import { SystemConfiguration } from '../../../core/domain_models/dependency_config';

interface Dependencies {
  registry: Registry;
  logger: Logger;
  config: SystemConfiguration;
}
import {
  OptimisticConcurrencyError,
  IntegrityViolationError,
  ConnectionError,
  EventStoreError,
} from './errors';

/** Postgres error code for serialization failures under SERIALIZABLE isolation. */
const PG_SERIALIZATION_FAILURE = '40001';
const PG_UNIQUE_VIOLATION = '23505';

/** Default number of retries for serialization failures before propagating the error. */
const DEFAULT_RETRIES = 3;

/**
 * EventRepository implementation for PostgreSQL-backed append-only event store.
 * Provides atomic append and versioned loading operations with cryptographic integrity checks.
 */
export class EventRepository<T extends Transaction> {
  private readonly poolManager: PostgresPoolManager;
  private readonly cryptoManager: CryptoManager;
  private readonly logger: Logger;
  
  // Performance metrics
  private readonly sqlExecutionDuration: Histogram<string>;
  private readonly cryptoVerificationDuration: Histogram<string>;

  constructor({ registry, logger, config }: Dependencies) {
    this.poolManager = PostgresPoolManager.getInstance(config);
    this.cryptoManager = CryptoManager.getInstance();
    this.logger = logger;

    this.sqlExecutionDuration = new Histogram({
      name: 'event_store_sql_execution_seconds',
      help: 'Duration of SQL operations in seconds',
      registers: [registry],
      labelNames: ['operation'],
    });

    this.cryptoVerificationDuration = new Histogram({
      name: 'event_store_crypto_verification_seconds',
      help: 'Duration of cryptographic verification in seconds',
      registers: [registry],
      labelNames: ['operation'],
    });
  }

  /**
   * Appends a new event to the aggregate stream.
   * Versioning is handled atomically at the database layer to ensure topological
   * consistency without depending on volatile external counters (Redis).
   * 
   * @param aggregateId Unique identifier for the aggregate stream.
   * @param event       The signed event envelope to persist.
   * @throws OptimisticConcurrencyError if a version conflict occurs.
   */
  public async append(
    aggregateId: string,
    event: MessageEnvelope<T>
  ): Promise<void> {
    const start = process.hrtime.bigint();
    
    await this.executeWithRetry(async (client) => {
      try {
        await client.query('BEGIN');

        // Insert event using subquery to calculate next version atomically.
        // The UNIQUE constraint on (aggregate_id, version) ensures that if two
        // concurrent writers both calculate version X, only one succeeds.
        await client.query(
          `INSERT INTO events (aggregate_id, version, event_type, metadata, payload, signature, created_at)
           VALUES ($1, (SELECT COALESCE(MAX(version), 0) + 1 FROM events WHERE aggregate_id = $1), $2, $3, $4, $5, NOW())`,
          [
            aggregateId,
            event.payload.type,
            canonicalizeJson(event.metadata),
            canonicalizeJson(event.payload),
            event.signature,
          ]
        );

        await client.query('COMMIT');
      } catch (err: any) {
        await client.query('ROLLBACK');
        
        // Map PostgreSQL unique_violation to OptimisticConcurrencyError.
        if (err.code === PG_UNIQUE_VIOLATION) {
          throw new OptimisticConcurrencyError(aggregateId, -1n);
        }
        
        throw err;
      }
    }, DEFAULT_RETRIES);

    this.recordDuration(this.sqlExecutionDuration, 'append', start);
  }

  /**
   * Loads the event stream for a specific aggregate ID.
   * Performs cryptographic verification on every loaded event.
   */
  public async load(aggregateId: string): Promise<MessageEnvelope<T>[]> {
    const client = await this.acquireClient();
    const start = process.hrtime.bigint();

    try {
      const { rows } = await client.query(
        'SELECT * FROM events WHERE aggregate_id = $1 ORDER BY version ASC',
        [aggregateId]
      );

      const envelopes: MessageEnvelope<T>[] = [];

      for (const row of rows) {
        const metadata = row.metadata;
        if (metadata && typeof metadata.createdAtNs === 'string') {
          (metadata as any).createdAtNs = BigInt(metadata.createdAtNs);
        }

        const envelope: MessageEnvelope<T> = {
          metadata: metadata,
          payload: row.payload as T,
          signature: row.signature,
        };

        const cryptoStart = process.hrtime.bigint();
        const dataToVerify = canonicalizeJson({
          metadata: envelope.metadata,
          payload: envelope.payload
        });
        
        const isValid = await this.cryptoManager.verifyEvent(dataToVerify, envelope.signature);
        this.recordDuration(this.cryptoVerificationDuration, 'verify', cryptoStart);

        if (!isValid) {
          throw new IntegrityViolationError(aggregateId, row.event_id, 'Cryptographic signature verification failed');
        }

        envelopes.push(envelope);
      }

      this.recordDuration(this.sqlExecutionDuration, 'load', start);
      return envelopes;
    } finally {
      client.release();
    }
  }

  /**
   * Executes a database operation with automatic retry on transient failures.
   */
  private async executeWithRetry(
    operation: (client: PoolClient) => Promise<void>,
    maxRetries: number
  ): Promise<void> {
    let attempt = 0;

    while (true) {
      const client = await this.acquireClient();
      try {
        await operation(client);
        return;
      } catch (err: any) {
        const isRetryable = err?.code === PG_SERIALIZATION_FAILURE;

        if (isRetryable && attempt < maxRetries) {
          attempt++;
          const delayMs = 50 * Math.pow(2, attempt) + Math.floor(Math.random() * 50);
          this.logger.warn({ code: err.code, attempt }, 'Transient DB error, retrying...');
          await new Promise((r) => setTimeout(r, delayMs));
          continue;
        }

        throw err;
      } finally {
        client.release();
      }
    }
  }

  private async acquireClient(): Promise<PoolClient> {
    try {
      return await this.poolManager.getPool().connect();
    } catch (err) {
      const stats = this.poolManager.getStats();
      throw new ConnectionError(err instanceof Error ? err.message : String(err), { ...stats });
    }
  }

  private recordDuration(histogram: Histogram<string>, label: string, start: bigint): void {
    const end = process.hrtime.bigint();
    const duration = Number(end - start) / 1e9;
    histogram.labels(label).observe(duration);
  }
}
