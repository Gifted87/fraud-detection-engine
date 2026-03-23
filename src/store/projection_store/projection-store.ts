import { Redis } from 'ioredis';
import { z } from 'zod';
import { Registry, Histogram } from 'prom-client';

/**
 * Zod schemas for input validation to mitigate injection attacks.
 */
const UserIdSchema = z.string().min(1);
const TransactionIdSchema = z.string().min(1);

/**
 * Custom Error for ProjectionStore operations.
 */
export class ProjectionStoreError extends Error {
  constructor(message: string, public readonly code: string) {
    super(message);
    this.name = 'ProjectionStoreError';
  }
}

/**
 * Singleton ProjectionStore managing high-concurrency read-model access.
 */
export class ProjectionStore {
  private static instance: ProjectionStore;
  private readonly redis: Redis;
  private readonly metrics: {
    redisLatency: Histogram<string>;
    cacheMisses: Histogram<string>;
  };

  private constructor(redisInstance: Redis, registry: Registry) {
    this.redis = redisInstance;
    this.metrics = {
      redisLatency: new Histogram({
        name: 'fraud_engine_redis_latency_seconds',
        help: 'Redis command execution latency',
        registers: [registry],
        labelNames: ['command'],
      }),
      cacheMisses: new Histogram({
        name: 'fraud_engine_cache_miss_total',
        help: 'Count of cache misses by entity type',
        registers: [registry],
        labelNames: ['entity_type'],
      }),
    };
  }

  /**
   * Initializes or returns the singleton instance.
   */
  public static initialize(redisInstance: Redis, registry: Registry): ProjectionStore {
    if (!ProjectionStore.instance) {
      ProjectionStore.instance = new ProjectionStore(redisInstance, registry);
    }
    return ProjectionStore.instance;
  }

  public static getInstance(): ProjectionStore {
    if (!ProjectionStore.instance) {
      throw new Error('ProjectionStore not initialized');
    }
    return ProjectionStore.instance;
  }

  /**
   * Atomically updates a user's balance and increments a sliding window counter.
   * Uses a Lua script to ensure consistency and minimize round-trips.
   */
  public async processTransaction(
    userId: string,
    amountDelta: bigint,
    transactionId: string,
    windowSizeSeconds: number
  ): Promise<bigint> {
    const validatedUserId = UserIdSchema.parse(userId);
    const validatedTxId = TransactionIdSchema.parse(transactionId);
    const now = Date.now();

    const luaScript = `
      local balanceKey = KEYS[1]
      local zsetKey = KEYS[2]
      local amountDelta = tonumber(ARGV[1])
      local txId = ARGV[2]
      local now = tonumber(ARGV[3])
      local windowStart = now - tonumber(ARGV[4]) * 1000

      -- Update Balance
      local newBalance = redis.call('INCRBY', balanceKey, amountDelta)

      -- Add to ZSET for sliding window
      redis.call('ZADD', zsetKey, now, txId)
      
      -- Remove old items
      redis.call('ZREMRANGEBYSCORE', zsetKey, '-inf', windowStart)

      return newBalance
    `;

    const start = process.hrtime.bigint();
    try {
      const result = await this.redis.eval(
        luaScript,
        2,
        `user:${validatedUserId}:balance`,
        `user:${validatedUserId}:tx_history`,
        amountDelta.toString(),
        validatedTxId,
        now.toString(),
        windowSizeSeconds.toString()
      );
      
      this.recordLatency('eval', start);
      return BigInt(result as number);
    } catch (err) {
      throw new ProjectionStoreError('Failed to execute atomic transaction update', 'EXECUTION_FAILED');
    }
  }

  /**
   * Retrieves the current balance for a user.
   */
  public async getBalance(userId: string): Promise<bigint> {
    const validatedUserId = UserIdSchema.parse(userId);
    const key = `user:${validatedUserId}:balance`;
    
    const start = process.hrtime.bigint();
    const result = await this.redis.get(key);
    this.recordLatency('get', start);

    if (result === null) {
      this.metrics.cacheMisses.labels('balance').observe(1);
      return 0n;
    }
    return BigInt(result);
  }

  /**
   * Retrieves the count of transactions in the sliding window.
   */
  public async getTransactionCount(userId: string): Promise<number> {
    const validatedUserId = UserIdSchema.parse(userId);
    const key = `user:${validatedUserId}:tx_history`;
    
    const start = process.hrtime.bigint();
    const result = await this.redis.zcard(key);
    this.recordLatency('zcard', start);
    
    return result;
  }

  private recordLatency(command: string, start: bigint): void {
    const end = process.hrtime.bigint();
    const duration = Number(end - start) / 1e9;
    this.metrics.redisLatency.labels(command).observe(duration);
  }
}
