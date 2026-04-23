import { RedisContainer, StartedRedisContainer } from '@testcontainers/redis';
import { Redis } from 'ioredis';
import { ProjectionStore } from '../../store/projection_store/projection-store';
import { Registry } from 'prom-client';

describe('ProjectionStore Integration', () => {
  let container: StartedRedisContainer;
  let redis: Redis;
  let projectionStore: ProjectionStore;

  beforeAll(async () => {
    // Audit fix: Explicitly provide image for Testcontainers
    // Increasing timeout to 120s for image pull and container start
    container = await new RedisContainer('redis:7-alpine').start();
    // Fix: use getConnectionUrl() instead of getConnectionString()
    redis = new Redis(container.getConnectionUrl());
    
    const registry = new Registry();
    projectionStore = ProjectionStore.initialize(redis, registry);
  }, 120000); 

  afterAll(async () => {
    if (redis) await redis.quit();
    if (container) await container.stop();
  });

  beforeEach(async () => {
    if (redis) await redis.flushall();
  });

  it('should atomically track transaction velocity using Lua scripts', async () => {
    const userId = 'user-1';
    const amount = 100n;
    const txId = 'tx-1';
    const windowSeconds = 60;

    await projectionStore.processTransaction(userId, amount, txId, windowSeconds);

    let count = await projectionStore.getTransactionCount(userId, windowSeconds);
    expect(count).toBe(1);

    await projectionStore.processTransaction(userId, amount, 'tx-2', windowSeconds);

    count = await projectionStore.getTransactionCount(userId, windowSeconds);
    expect(count).toBe(2);
  });

  it('should correctly handle sliding window expiration', async () => {
    const userId = 'user-2';
    const windowSeconds = 1; 

    await projectionStore.processTransaction(userId, 100n, 'tx-old', windowSeconds);
    
    await new Promise(resolve => setTimeout(resolve, 1100));

    const count = await projectionStore.getTransactionCount(userId, windowSeconds);
    expect(count).toBe(0);
  });
});
