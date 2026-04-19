import { CryptoValidator, InitializationError, CryptographicOperationError } from './crypto-validator.service';

describe('CryptoValidator', () => {
  beforeEach(() => {
    // Reset singleton instance by setting it to null
    (CryptoValidator as any).instance = null;
  });

  it('should initialize correctly with a valid signing key', () => {
    CryptoValidator.initialize('my-secret-key');
    const instance = CryptoValidator.getInstance();
    expect(instance).toBeDefined();
  });

  it('should throw InitializationError if initialized with an empty key', () => {
    expect(() => CryptoValidator.initialize('')).toThrow(InitializationError);
  });

  it('should throw InitializationError if getInstance is called before initialize', () => {
    expect(() => CryptoValidator.getInstance()).toThrow(InitializationError);
  });

  it('should correctly sign data and verify valid signatures', async () => {
    CryptoValidator.initialize('test-key');
    const validator = CryptoValidator.getInstance();
    
    const data = JSON.stringify({ payload: 'test' });
    const signature = await validator.sign(data);
    
    expect(signature).toBeDefined();
    expect(typeof signature).toBe('string');
    
    const isValid = await validator.verify(data, signature);
    expect(isValid).toBe(true);
  });

  it('should reject invalid signatures', async () => {
    CryptoValidator.initialize('test-key');
    const validator = CryptoValidator.getInstance();
    
    const data = JSON.stringify({ payload: 'test' });
    const invalidSignature = 'a'.repeat(64); // random 64 char hex string
    
    const isValid = await validator.verify(data, invalidSignature);
    expect(isValid).toBe(false);
  });

  it('should reject structurally invalid signatures without throwing', async () => {
    CryptoValidator.initialize('test-key');
    const validator = CryptoValidator.getInstance();
    
    const isValid = await validator.verify(JSON.stringify({}), 'not-even-hex');
    expect(isValid).toBe(false);
  });
});
