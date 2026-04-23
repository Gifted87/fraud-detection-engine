import { TransactionFactory } from './index';
import { CryptoManager } from '../../utils/security/crypto';

describe('TransactionFactory', () => {
  beforeAll(() => {
    (CryptoManager as any).instance = null;
    CryptoManager.initialize(
      'test-enc-key',
      'test-salt-long-enough',
      '-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEA1PSK//wZg2vQhgUBDn2HvG0Y8IVau0iGjFBw4TBvGSc=\n-----END PUBLIC KEY-----',
      '-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIC8Wb4tn/NTE4alDqkPL/Hgd7V9fE4rUCN3JqC4wHMn9\n-----END PRIVATE KEY-----'
    );
  });

  it('should create a TransactionInitiated envelope', async () => {
    const envelope = await TransactionFactory.createTransactionInitiated(
      'tx123' as any,
      'u1' as any,
      'm1' as any,
      { value: 50n, currency: 'USD' } as any,
      { latitude: 0, longitude: 0, ipAddress: '127.0.0.1', deviceFingerprint: 'f', userAgent: 'u' } as any
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
      { latitude: 0, longitude: 0, ipAddress: '127.0.0.1', deviceFingerprint: 'f', userAgent: 'u' } as any,
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
      { latitude: 0, longitude: 0, ipAddress: '127.0.0.1', deviceFingerprint: 'f', userAgent: 'u' } as any,
      'Suspicious Activity',
      0.95
    );
    
    expect(event.type).toBe('TransactionFlagged');
    expect(event.reason).toBe('Suspicious Activity');
    expect(event.riskScore).toBe(0.95);
    expect((event as any).signature).toBeUndefined();
  });
});
