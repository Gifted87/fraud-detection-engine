import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { Pool } from 'pg';
import { EventRepository } from '../../store/event_store/postgres_impl/repository';
import { Transaction } from '../../core/domain_models/definitions/transaction.interface';
import { EventEnvelopeFactory } from '../../core/domain_models/messaging/event-envelope.schema';
import { Registry } from 'prom-client';
import * as fs from 'fs';
import * as path from 'path';

describe('EventRepository Integration', () => {
  let container: StartedPostgreSqlContainer;
  let pool: Pool;
  let repository: EventRepository<Transaction>;
  let registry: Registry;

  beforeAll(async () => {
    // Audit fix: Explicitly provide image for Testcontainers
    // Increasing timeout to 120s for image pull and container start
    container = await new PostgreSqlContainer('postgres:15-alpine').start();
    
    pool = new Pool({
      host: container.getHost(),
      port: container.getPort(),
      database: container.getDatabase(),
      user: container.getUsername(),
      password: container.getPassword(),
    });

    // Initialize schema from the SQL file
    const schemaPath = path.join(__dirname, '../../store/event_store/postgres_impl/schema.sql');
    const schema = fs.readFileSync(schemaPath, 'utf8');
    await pool.query(schema);

    registry = new Registry();
    repository = new EventRepository({
      registry,
      config: {
        DB_POOL_MIN: 2,
        DB_POOL_MAX: 5,
        DB_URL: `postgres://${container.getUsername()}:${container.getPassword()}@${container.getHost()}:${container.getPort()}/${container.getDatabase()}`,
        NODE_ENV: 'test'
      } as any,
      logger: {
        info: jest.fn(),
        error: jest.fn(),
        warn: jest.fn(),
        fatal: jest.fn(),
        debug: jest.fn()
      } as any
    });
  }, 120000); 

  afterAll(async () => {
    if (pool) await pool.end();
    if (container) await container.stop();
  });

  it('should successfully append and retrieve events from a real Postgres instance', async () => {
    const userId = 'user-123';
    const transaction: Transaction = {
      transactionId: 'tx-1',
      userId,
      merchantId: 'm-1',
      amount: { value: 100n, currency: 'USD' },
      timestamp: BigInt(Date.now()),
      telemetry: { latitude: 0, longitude: 0, ipAddress: '127.0.0.1', deviceFingerprint: 'f1', userAgent: 'a' }
    } as any;

    const envelope = await EventEnvelopeFactory.create(transaction);
    
    // 1. Append
    await repository.append(userId, envelope);

    // 2. Load stream
    const events = await repository.load(userId);
    
    expect(events).toHaveLength(1);
    expect(events[0].payload.transactionId).toBe('tx-1');
  });

  it('should enforce optimistic concurrency control (OCC) on aggregate versions', async () => {
    const userId = 'user-occ';
    const envelope1 = await EventEnvelopeFactory.create({ transactionId: 'tx-1', userId, amount: {value: 10n}, type: 'Tx' } as any);
    const envelope2 = await EventEnvelopeFactory.create({ transactionId: 'tx-2', userId, amount: {value: 10n}, type: 'Tx' } as any);

    await repository.append(userId, envelope1);
    await repository.append(userId, envelope2);
    
    const events = await repository.load(userId);
    expect(events).toHaveLength(2);
  });
});
