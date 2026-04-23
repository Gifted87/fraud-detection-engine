import { AlertingSubsystem } from './alerts';
import { TransactionFactory } from '../../../../core/domain_models';

describe('AlertingSubsystem', () => {
  let mockRedis: any;
  let mockKafkaClient: any;
  let mockMetrics: any;
  let mockLogger: any;
  let alerts: AlertingSubsystem;

  beforeEach(() => {
    mockRedis = {
      set: jest.fn(),
      del: jest.fn().mockResolvedValue(1)
    };
    mockKafkaClient = {
      publish: jest.fn()
    };
    mockMetrics = {
      incrementThroughput: jest.fn(),
      observeLatency: jest.fn()
    };
    mockLogger = {
      info: jest.fn(),
      error: jest.fn(),
      fatal: jest.fn(),
      debug: jest.fn()
    };
    
    alerts = new AlertingSubsystem({
      redis: mockRedis,
      kafkaClient: mockKafkaClient,
      metricsCollector: mockMetrics,
      logger: mockLogger,
      config: { NODE_ENV: 'test' } as any
    });
    
    TransactionFactory.createTransactionFlagged = jest.fn().mockResolvedValue({ payload: 'test_payload' });
  });

  it('should skip duplicate alerts using Redis SETNX', async () => {
    mockRedis.set.mockResolvedValueOnce(null); // Indicates key already existed
    await alerts.dispatchFlag('tx1' as any, 'u1' as any, 'm1' as any, {} as any, {} as any, 'reason', 0.9, 'v1');
    expect(mockMetrics.incrementThroughput).toHaveBeenCalledWith(expect.anything(), 'alert_deduplication_skipped');
    expect(mockKafkaClient.publish).not.toHaveBeenCalled();
  });

  it('should publish flag to Kafka if unique', async () => {
    mockRedis.set.mockResolvedValueOnce('OK'); // Successfully set

    await alerts.dispatchFlag('tx1' as any, 'u1' as any, 'm1' as any, {} as any, {} as any, 'reason', 0.9, 'v1');
    
    expect(mockKafkaClient.publish).toHaveBeenCalledWith('fraud-alerts-high-priority', 'test_payload');
    expect(mockMetrics.incrementThroughput).toHaveBeenCalledWith(expect.anything(), 'total_alerts_sent');
  });

  it('should throw error and release lock if kafka dispatch fails', async () => {
    mockRedis.set.mockResolvedValueOnce('OK');
    mockKafkaClient.publish.mockRejectedValueOnce(new Error('kafka failure'));

    await expect(
      alerts.dispatchFlag('tx1' as any, 'u1' as any, 'm1' as any, {} as any, {} as any, 'reason', 0.9, 'v1')
    ).rejects.toThrow('kafka failure');

    // Verification of architectural fix: lock MUST be released on failure
    expect(mockRedis.del).toHaveBeenCalledWith('fraud:alert:processed:tx1');
    expect(mockMetrics.incrementThroughput).toHaveBeenCalledWith(expect.anything(), 'kafka_producer_error');
  });
});
