import { TransactionFactory } from './index';
import { CryptoValidator } from './security/crypto-validator.service';

describe('TransactionFactory', () => {
  beforeAll(() => {
    (CryptoValidator as any).instance = null;
    CryptoValidator.initialize('test-key');
  });

  const baseArgs = [
    'userid-123' as any,
    'user-from' as any,
    'merch-456' as any,
    { value: 100n, currency: 'USD' } as any,
    { latitude: 0, longitude: 0, ipAddress: '0.0.0.0' } as any,
  ] as const;

  it('should create a TransactionInitiated envelope', async () => {
    const envelope = await TransactionFactory.createTransactionInitiated(
      'tx123' as any,
      'u1' as any,
      'm1' as any,
      { value: 50n, currency: 'USD' } as any,
      {} as any
    );
    expect(envelope.payload.type).toBe('TransactionInitiated');
    expect(envelope.payload.transactionId).toBe('tx123');
    expect(envelope.signature).toBeDefined();
  });

  it('should create a TransactionValidated envelope', async () => {
    const envelope = await TransactionFactory.createTransactionValidated(
      'tx123' as any,
      'u1' as any,
      'm1' as any,
      { value: 50n, currency: 'USD' } as any,
      {} as any,
      'validator-999'
    );
    expect(envelope.payload.type).toBe('TransactionValidated');
    expect(envelope.payload.validatorId).toBe('validator-999');
  });

  it('should format buildTransactionFlagged without signing', () => {
    const event = TransactionFactory.buildTransactionFlagged(
      'tx123' as any,
      'u1' as any,
      'm1' as any,
      { value: 50n, currency: 'USD' } as any,
      {} as any,
      'Suspicious Activity',
      0.95
    );
    
    expect(event.type).toBe('TransactionFlagged');
    expect(event.reason).toBe('Suspicious Activity');
    expect(event.riskScore).toBe(0.95);
    // As an inner domain object it shouldn't have an envelope signature at the root level.
    expect((event as any).signature).toBeUndefined();
  });
});
