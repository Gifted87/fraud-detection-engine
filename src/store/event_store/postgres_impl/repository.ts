import { PoolClient } from 'pg';
import { Registry, Histogram } from 'prom-client';
import { PostgresPoolManager } from './client';
import { CryptoValidator } from '../../../core/domain_models/security/crypto-validator.service';
import { MessageEnvelope } from '../../../core/domain_models/messaging/event-envelope.messaging';
import { Transaction } from '../../../core/domain_models/definitions/transaction.interface';
import {
  OptimisticConcurrencyError,
  IntegrityViolationError,
  ConnectionError,
} from './errors';

/**
 * EventRepository implementation for PostgreSQL-backed append-only event store.
 * Provides atomic append and versioned loading operations with cryptographic integrity checks.
 */
export class EventRepository<T extends Transaction> {
  private readonly poolManager: PostgresPoolManager;
  private readonly cryptoValidator: CryptoValidator;
  
  // Performance metrics
  private readonly sqlExecutionDuration: Histogram<string>;
  private readonly cryptoVerificationDuration: Histogram<string>;

  constructor(registry: Registry) {
    this.poolManager = PostgresPoolManager.getInstance();
    this.cryptoValidator = CryptoValidator.getInstance();

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
    });
  }

  /**
   * Appends a new event to the stream for a given aggregate.
   * Enforces optimistic concurrency and transactional integrity.
   */
  public async append(aggregateId: string, event: MessageEnvelope<T>, version: bigint): Promise<void> {
    const client = await this.acquireClient();
    const start = process.hrtime.bigint();

    try {
      await client.query('BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE');

      // Check current version for optimistic concurrency
      const { rows } = await client.query(
        'SELECT version FROM events WHERE aggregate_id = $1 ORDER BY version DESC LIMIT 1 FOR UPDATE',
        [aggregateId]
      );

      const currentVersion = rows.length > 0 ? BigInt(rows[0].version) : 0n;
      if (version !== currentVersion + 1n) {
        throw new OptimisticConcurrencyError(aggregateId, version, currentVersion + 1n);
      }

      await client.query(
        `INSERT INTO events (aggregate_id, version, event_type, payload, signature, created_at)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [
          aggregateId,
          version.toString(),
          event.payload.type,
          JSON.stringify(event.payload),
          event.signature,
        ]
      );

      await client.query('COMMIT');
      this.recordDuration(this.sqlExecutionDuration, 'append', start);
    } catch (err) {
      await client.query('ROLLBACK');
      throw err;
    } finally {
      client.release();
    }
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
        const envelope: MessageEnvelope<T> = {
          metadata: {
            schemaVersion: 'v1.0',
            createdAtNs: BigInt(new Date(row.created_at).getTime() * 1_000_000),
            provenanceTrace: 'db_load',
          },
          payload: row.payload as T,
          signature: row.signature,
        };

        // Cryptographic verification
        const cryptoStart = process.hrtime.bigint();
        const dataToVerify = JSON.stringify({ metadata: envelope.metadata, payload: envelope.payload });
        const isValid = await this.cryptoValidator.verify(dataToVerify, envelope.signature);
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

  private async acquireClient(): Promise<PoolClient> {
    try {
      return await this.poolManager.getPool().connect();
    } catch (err) {
      throw new ConnectionError(err instanceof Error ? err.message : 'Unknown connection error', this.poolManager.getStats());
    }
  }

  private recordDuration(histogram: Histogram<string>, label: string, start: bigint): void {
    const end = process.hrtime.bigint();
    const duration = Number(end - start) / 1e9;
    histogram.labels(label).observe(duration);
  }
}
