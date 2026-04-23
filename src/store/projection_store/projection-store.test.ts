import { Redis } from 'ioredis';
import { Registry } from 'prom-client';
import { ProjectionStore, ProjectionStoreError } from './projection-store';

describe('ProjectionStore', () => {
  let redisMock: jest.Mocked<Redis>;
  let registry: Registry;
  let store: ProjectionStore;

  beforeEach(() => {
    redisMock = {
      eval: jest.fn(),
      get: jest.fn(),
      zcount: jest.fn(),
    } as any;
    
    registry = new Registry();
    (ProjectionStore as any).instance = undefined;
    store = ProjectionStore.initialize(redisMock, registry);
  });

  it('should initialize and return singleton', () => {
    expect(store).toBeDefined();
    expect(ProjectionStore.getInstance()).toBe(store);
  });

  it('should process transaction using eval successfully', async () => {
    redisMock.eval.mockResolvedValueOnce(500); // New balance mock
    
    const balance = await store.processTransaction('user1', 100n, 'tx1', 60);
    expect(balance).toBe(500n);
    expect(redisMock.eval).toHaveBeenCalled();
  });

  it('should be idempotent when processing same transactionId', async () => {
    redisMock.eval.mockResolvedValueOnce('100');
    const result1 = await store.processTransaction('user-1', 100n, 'tx-16', 60);
    
    redisMock.eval.mockResolvedValueOnce('100');
    const result2 = await store.processTransaction('user-1', 100n, 'tx-16', 60);

    expect(result1).toBe(100n);
    expect(result2).toBe(100n);
    expect(redisMock.eval).toHaveBeenCalledTimes(2);
  });

  it('should throw ProjectionStoreError on eval failure', async () => {
    redisMock.eval.mockRejectedValueOnce(new Error('redis error'));
    
    await expect(store.processTransaction('user1', 10n, 'tx1', 60))
      .rejects.toThrow(ProjectionStoreError);
  });

  it('should get balance successfully', async () => {
    redisMock.get.mockResolvedValueOnce('1500');
    
    const balance = await store.getBalance('user1');
    expect(balance).toBe(1500n);
    expect(redisMock.get).toHaveBeenCalledWith('user:user1:balance');
  });

  it('should return 0n if balance is missing', async () => {
    redisMock.get.mockResolvedValueOnce(null);
    
    const balance = await store.getBalance('user1');
    expect(balance).toBe(0n);
  });

  it('should get transaction count successfully', async () => {
    redisMock.zcount.mockResolvedValueOnce(5);
    
    const count = await store.getTransactionCount('user1', 60);
    expect(count).toBe(5);
    expect(redisMock.zcount).toHaveBeenCalled();
  });

  it('should validate zod inputs and throw if invalid', async () => {
    await expect(store.getBalance('')).rejects.toThrow();
  });
});
