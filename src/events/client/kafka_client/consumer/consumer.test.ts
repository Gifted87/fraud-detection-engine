import { FraudEventConsumer } from './consumer';
import { KafkaConfigProvider } from '../config/kafka-config';
import { MetricsManager } from '../telemetry/metrics';
import { ProjectionStore } from '../../../../store/projection_store/projection-store';
import { EventEnvelopeFactory } from '../../../../core/domain_models/messaging/event-envelope.schema';
import { Kafka } from 'kafkajs';

jest.mock('kafkajs');

describe('FraudEventConsumer', () => {
  let mockConsumer: any;
  let mockProducer: any;
  let mockKafka: any;

  beforeAll(() => {
    (KafkaConfigProvider as any).instance = {
      getConfig: () => ({ clientId: 'c1', brokers: ['host'], consumer: {} })
    };
    (MetricsManager as any).instance = {
      recordRebalance: jest.fn(),
      recordConsumerLatency: jest.fn(),
      recordDlqIngress: jest.fn()
    };
    (ProjectionStore as any).instance = {
      processTransaction: jest.fn().mockResolvedValue(100n)
    };
  });

  beforeEach(() => {
    mockConsumer = {
      connect: jest.fn(),
      subscribe: jest.fn(),
      run: jest.fn(),
      disconnect: jest.fn(),
      on: jest.fn()
    };
    mockProducer = {
      send: jest.fn()
    };
    mockKafka = {
      consumer: jest.fn().mockReturnValue(mockConsumer),
      producer: jest.fn().mockReturnValue(mockProducer),
    };
    (Kafka as jest.Mock).mockImplementation(() => mockKafka);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should initialize and connect', async () => {
    const consumer = new FraudEventConsumer('test-topic');
    mockConsumer.run.mockResolvedValueOnce(undefined);
    
    await consumer.start();
    expect(mockConsumer.connect).toHaveBeenCalled();
    expect(mockConsumer.subscribe).toHaveBeenCalledWith({ topic: 'test-topic', fromBeginning: false });
    expect(mockConsumer.run).toHaveBeenCalled();
  });

  it('should disconnect on shutdown', async () => {
    const consumer = new FraudEventConsumer('test-topic');
    await consumer.shutdown();
    expect(mockConsumer.disconnect).toHaveBeenCalled();
  });

  it('should process a valid message batch and call processor', async () => {
    const processor = jest.fn().mockResolvedValue(undefined);
    const consumer = new FraudEventConsumer('test-topic', 'dlq', processor);
    
    let eachBatchCb: any;
    mockConsumer.run.mockImplementation(async ({ eachBatch }: any) => {
      eachBatchCb = eachBatch;
    });

    await consumer.start();

    // Mock verifyEnvelope to pass
    jest.spyOn(EventEnvelopeFactory, 'verifyEnvelope').mockResolvedValue(true);

    const validEnvelope = {
      metadata: { schemaVersion: 'v1.0' },
      payload: {
        type: 'TransactionInitiated',
        userId: 'u1',
        merchantId: 'm1',
        transactionId: 'tx1',
        amount: { value: 50n, currency: 'USD' },
        timestamp: 123n,
        telemetry: { 
          latitude: 0, 
          longitude: 0, 
          ipAddress: '127.0.0.1',
          deviceFingerprint: 'fps123',
          userAgent: 'test-agent'
        }
      },
      signature: 'sig'
    };

    const payload = {
      batch: {
        messages: [{ value: Buffer.from(JSON.stringify(validEnvelope, (k,v) => typeof v === 'bigint' ? v.toString() : v)) }]
      },
      resolveOffset: jest.fn(),
      heartbeat: jest.fn(),
      commitOffsetsIfNecessary: jest.fn()
    };

    await eachBatchCb(payload);

    // Should call projectionStore processing and the custom processor
    expect(ProjectionStore.getInstance().processTransaction).toHaveBeenCalledWith('u1', 50n, 'tx1', 60);
    expect(processor).toHaveBeenCalled();
    expect(payload.resolveOffset).toHaveBeenCalled();
  });

  it('should route invalid messages to DLQ', async () => {
    const consumer = new FraudEventConsumer('test-topic');
    
    let eachBatchCb: any;
    mockConsumer.run.mockImplementation(async ({ eachBatch }: any) => {
      eachBatchCb = eachBatch;
    });

    await consumer.start();

    // Mock verifyEnvelope to throw
    jest.spyOn(EventEnvelopeFactory, 'verifyEnvelope').mockRejectedValue(new Error('tampered'));

    const invalidEnvelope = { value: Buffer.from(JSON.stringify({})) };

    const payload = {
      batch: { messages: [{ key: 'k1', value: invalidEnvelope.value }] },
      resolveOffset: jest.fn(),
      heartbeat: jest.fn(),
      commitOffsetsIfNecessary: jest.fn()
    };

    await eachBatchCb(payload);

    expect(mockProducer.send).toHaveBeenCalled();
    expect(MetricsManager.getInstance().recordDlqIngress).toHaveBeenCalled();
    expect(payload.resolveOffset).toHaveBeenCalled();
  });
});
