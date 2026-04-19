import { Redis } from 'ioredis';
import { GeospatialRule } from './geospatial_rule';
import { MetricsCollector } from '../../../../../utils/metrics/metrics-collector';
import { Transaction } from '../../../../../core/domain_models/definitions/transaction.interface';
import { Registry } from 'prom-client';

describe('GeospatialRule', () => {
  let redis: jest.Mocked<Redis>;
  let metrics: MetricsCollector;
  let rule: GeospatialRule;

  beforeEach(() => {
    redis = {
      get: jest.fn(),
      set: jest.fn(),
    } as any;
    
    const registry = new Registry();
    metrics = MetricsCollector.initialize(registry);
    rule = new GeospatialRule(redis, metrics);
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should flag impossible travel', async () => {
    const userId = 'user-1';
    const tx1Timestamp = 1000000000000000000n; // 1s
    const tx2Timestamp = 1000000000000000000n + BigInt(0.5 * 1e9 * 3600); // +0.5 hour
    
    const lastState = {
      telemetry: {
        latitude: 40.7128, // NY
        longitude: -74.0060,
      },
      timestamp: tx1Timestamp.toString(),
    };

    redis.get.mockResolvedValueOnce(JSON.stringify(lastState));

    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-2',
      userId,
      merchantId: 'm-1',
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: tx2Timestamp,
      telemetry: {
        latitude: 48.8566, // Paris (~5800km from NY)
        longitude: 2.3522,
      } as any,
    } as any;

    const result = await rule.evaluate(transaction);

    // Distance NY to Paris ~5837 km. Time 0.5h. Speed ~11674 km/h (> 900)
    expect(result.isSuspicious).toBe(true);
    expect(result.riskScore).toBe(0.9);
    expect(result.reason).toContain('Impossible travel detected');
  });

  it('should not flag normal travel', async () => {
    const userId = 'user-1';
    const tx1Timestamp = 1000000000000000000n; // 1s
    const tx2Timestamp = 1000000000000000000n + BigInt(10 * 1e9 * 3600); // +10 hours
    
    const lastState = {
      telemetry: {
        latitude: 40.7128, // NY
        longitude: -74.0060,
      },
      timestamp: tx1Timestamp.toString(),
    };

    redis.get.mockResolvedValueOnce(JSON.stringify(lastState));

    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-2',
      userId,
      merchantId: 'm-1',
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: tx2Timestamp,
      telemetry: {
        latitude: 48.8566, // Paris
        longitude: 2.3522,
      } as any,
    } as any;

    const result = await rule.evaluate(transaction);

    // Speed ~583.7 km/h (< 900)
    expect(result.isSuspicious).toBe(false);
    expect(result.riskScore).toBe(0.0);
  });

  it('should handle warm-up phase (no previous telemetry)', async () => {
    redis.get.mockResolvedValueOnce(null);

    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-1',
      userId: 'user-1',
      merchantId: 'm-1',
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: BigInt(Date.now()),
      telemetry: {
        latitude: 40.7128,
        longitude: -74.0060,
      } as any,
    } as any;

    const result = await rule.evaluate(transaction);

    expect(result.isSuspicious).toBe(false);
    expect(result.reason).toContain('warm-up phase');
    expect(redis.set).toHaveBeenCalled();
  });
});
