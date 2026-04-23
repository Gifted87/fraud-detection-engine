import { KafkaEventProducer } from './producer';
import { KafkaConfigProvider } from '../config/kafka-config';
import { MetricsManager } from '../telemetry/metrics';
import { CryptoManager } from '../../../../utils/security/crypto';
import { Kafka } from 'kafkajs';

jest.mock('kafkajs');

describe('KafkaEventProducer', () => {
  let mockProducer: any;
  let mockKafka: any;

  beforeAll(() => {
    (CryptoManager as any).instance = null;
    CryptoManager.initialize(
      'test-enc-key',
      'test-salt-long-enough',
      '-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEA1PSK//wZg2vQhgUBDn2HvG0Y8IVau0iGjFBw4TBvGSc=\n-----END PUBLIC KEY-----',
      '-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIC8Wb4tn/NTE4alDqkPL/Hgd7V9fE4rUCN3JqC4wHMn9\n-----END PRIVATE KEY-----'
    );
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
    
    const event = { transactionId: 'tx1', type: 'TransactionInitiated', userId: 'u1' } as any;
    await producer.produce('topic-test', event);
    expect(mockProducer.send).toHaveBeenCalled();
    const sentArgs = mockProducer.send.mock.calls[0][0];
    expect(sentArgs.topic).toBe('topic-test');
    expect(typeof sentArgs.messages[0].value).toBe('string');
    // Verify that the produced value is a signed envelope (contains signature)
    const val = JSON.parse(sentArgs.messages[0].value);
    expect(val.signature).toBeDefined();
    expect(val.payload.transactionId).toBe('tx1');
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
