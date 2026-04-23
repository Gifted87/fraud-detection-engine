import { Registry } from 'prom-client';
import { VelocityRule } from './velocity-rule';
import { ProjectionStore } from '../../../../../store/projection_store/projection-store';
import { Transaction } from '../../../../../core/domain_models/definitions/transaction.interface';

describe('VelocityRule', () => {
  let projectionStore: jest.Mocked<ProjectionStore>;
  let rule: VelocityRule;
  let mockLogger: any;

  beforeEach(() => {
    projectionStore = {
      getTransactionCount: jest.fn(),
    } as any;
    
    const registry = new Registry();
    mockLogger = {
      info: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
      fatal: jest.fn(),
      debug: jest.fn()
    };

    rule = new VelocityRule({
      projectionStore: projectionStore as any,
      registry,
      logger: mockLogger,
      config: {
        NODE_ENV: 'test',
        VELOCITY_THRESHOLD: 10,
        VELOCITY_WINDOW_SECONDS: 60
      } as any
    });
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should flag suspicious velocity', async () => {
    const userId = 'user-1';
    projectionStore.getTransactionCount.mockResolvedValueOnce(11);

    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-12',
      userId,
      merchantId: 'm-1',
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: BigInt(Date.now()),
      telemetry: {} as any,
    } as any;

    const result = await rule.evaluate(transaction);

    expect(result.isSuspicious).toBe(true);
    expect(result.riskScore).toBe(0.8);
    expect(result.reason).toContain('exceeded velocity threshold of 10');
  });

  it('should not flag normal velocity', async () => {
    const userId = 'user-1';
    projectionStore.getTransactionCount.mockResolvedValueOnce(5);

    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-6',
      userId,
      merchantId: 'm-1',
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: BigInt(Date.now()),
      telemetry: {} as any,
    } as any;

    const result = await rule.evaluate(transaction);

    expect(result.isSuspicious).toBe(false);
    expect(result.riskScore).toBe(0.0);
  });
});
