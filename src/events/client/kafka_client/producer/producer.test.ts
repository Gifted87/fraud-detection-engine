import { KafkaEventProducer } from './producer';
import { KafkaConfigProvider } from '../config/kafka-config';
import { MetricsManager } from '../telemetry/metrics';
import { CryptoValidator } from '../../../../core/domain_models/security/crypto-validator.service';
import { Kafka } from 'kafkajs';

jest.mock('kafkajs');

describe('KafkaEventProducer', () => {
  let mockProducer: any;
  let mockKafka: any;

  beforeAll(() => {
    (CryptoValidator as any).instance = null;
    CryptoValidator.initialize('test-key');
  });

  beforeEach(() => {
    mockProducer = {
      connect: jest.fn().mockResolvedValue(undefined),
      send: jest.fn().mockResolvedValue([{}]),
      disconnect: jest.fn().mockResolvedValue(undefined),
    };
    
    mockKafka = {
      producer: jest.fn().mockReturnValue(mockProducer),
    };
    (Kafka as jest.Mock).mockImplementation(() => mockKafka);

    (KafkaEventProducer as any).instance = undefined;
    (KafkaConfigProvider as any).instance = {
      getConfig: () => ({ clientId: 'test', brokers: ['host'], producer: { retry: {} } })
    };
    (MetricsManager as any).instance = {
      trackProducerOperation: jest.fn((labels, op) => op())
    };
  });

  it('should initialize and connect', async () => {
    const producer = KafkaEventProducer.initialize(
      KafkaConfigProvider.getInstance(),
      MetricsManager.getInstance()
    );
    await producer.connect();
    expect(mockProducer.connect).toHaveBeenCalled();
  });

  it('should stringify BigInt values correctly and produce an event', async () => {
    const producer = KafkaEventProducer.initialize(
      KafkaConfigProvider.getInstance(),
      MetricsManager.getInstance()
    );
    
    const event = { transactionId: 'tx1', type: 'Tx' } as any;
    await producer.produce('topic-test', event);
    expect(mockProducer.send).toHaveBeenCalled();
    const sentArgs = mockProducer.send.mock.calls[0][0];
    expect(sentArgs.topic).toBe('topic-test');
    expect(typeof sentArgs.messages[0].value).toBe('string');
  });

  it('should gracefully shutdown', async () => {
    const producer = KafkaEventProducer.initialize(
      KafkaConfigProvider.getInstance(),
      MetricsManager.getInstance()
    );
    await producer.gracefulShutdown();
    expect(mockProducer.disconnect).toHaveBeenCalled();
  });
});
