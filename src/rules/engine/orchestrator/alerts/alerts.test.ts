import { AlertingSubsystem } from './alerts';
import { TransactionFactory } from '../../../../core/domain_models';

describe('AlertingSubsystem', () => {
  let mockRedis: any;
  let mockKafkaClient: any;
  let mockMetrics: any;
  let alerts: AlertingSubsystem;

  beforeEach(() => {
    mockRedis = {
      set: jest.fn()
    };
    mockKafkaClient = {
      publish: jest.fn()
    };
    mockMetrics = {
      incrementThroughput: jest.fn(),
      observeLatency: jest.fn()
    };
    
    (AlertingSubsystem as any).instance = undefined;
    alerts = AlertingSubsystem.initialize(mockRedis, mockKafkaClient, mockMetrics, { idempotencyTtlSeconds: 10 });
    
    TransactionFactory.createTransactionFlagged = jest.fn().mockResolvedValue({ payload: 'test_payload' });
  });

  it('should skip duplicate alerts using Redis SETNX', async () => {
    mockRedis.set.mockResolvedValueOnce(null); // Indicates key already existed
    hasSkipped:
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

  it('should throw error and track metrics if kafka dispatch fails', async () => {
    mockRedis.set.mockResolvedValueOnce('OK');
    mockKafkaClient.publish.mockRejectedValueOnce(new Error('kafka failure'));

    await expect(
      alerts.dispatchFlag('tx1' as any, 'u1' as any, 'm1' as any, {} as any, {} as any, 'reason', 0.9, 'v1')
    ).rejects.toThrow('kafka failure');

    expect(mockMetrics.incrementThroughput).toHaveBeenCalledWith(expect.anything(), 'kafka_producer_error');
  });
});
