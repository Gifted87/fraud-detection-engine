import { EventEnvelopeFactory, SecurityViolationError, MessageEnvelope } from './event-envelope.schema';
import { CryptoValidator } from '../security/crypto-validator.service';
import { Transaction } from '../definitions/transaction.interface';

describe('EventEnvelopeFactory', () => {
  beforeAll(() => {
    // Reset and Initialize CryptoValidator for EventEnvelopeFactory
    (CryptoValidator as any).instance = null;
    CryptoValidator.initialize('test-signing-key');
  });

  it('should create and sign a message envelope successfully', async () => {
    const mockTransaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-123',
      userId: 'user-456',
      merchantId: 'merch-789',
      amount: { value: 1000n, currency: 'USD' } as any,
      timestamp: BigInt(Date.now()),
      telemetry: { latitude: 0, longitude: 0, ipAddress: '127.0.0.1' } as any,
    } as any;

    const envelope = await EventEnvelopeFactory.create(mockTransaction);
    
    expect(envelope).toBeDefined();
    expect(envelope.signature).toBeDefined();
    expect(envelope.metadata).toBeDefined();
    expect(envelope.metadata.schemaVersion).toBe('v1.0');
    expect(envelope.payload).toEqual(mockTransaction);
    
    // Verify payload is wrapped frozen (note Object.isFrozen checks shallow freeze)
    expect(Object.isFrozen(envelope)).toBe(true);
  });

  it('should successfully verify a valid envelope', async () => {
    const mockTransaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-test',
      userId: 'user-test',
      merchantId: 'merch-test',
      amount: { value: 500n, currency: 'USD' } as any,
      timestamp: 123456789n,
      telemetry: { latitude: 0, longitude: 0, ipAddress: '127.0.0.1' } as any,
    } as any;

    const envelope = await EventEnvelopeFactory.create(mockTransaction);
    const isValid = await EventEnvelopeFactory.verifyEnvelope(envelope);
    expect(isValid).toBe(true);
  });

  it('should throw SecurityViolationError when verifying a tampered envelope payload', async () => {
    const mockTransaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-2',
      userId: 'u',
      merchantId: 'm',
      amount: { value: 10n, currency: 'USD' } as any,
      timestamp: 1n,
      telemetry: { latitude: 0, longitude: 0 } as any,
    } as any;

    const envelope = await EventEnvelopeFactory.create(mockTransaction);
    
    // Clone and tamper (bypass freeze)
    const tamperedEnvelope: MessageEnvelope<any> = {
      ...envelope,
      payload: { ...envelope.payload, amount: { value: 9999n, currency: 'USD' } }
    };

    await expect(EventEnvelopeFactory.verifyEnvelope(tamperedEnvelope)).rejects.toThrow(SecurityViolationError);
  });
});
