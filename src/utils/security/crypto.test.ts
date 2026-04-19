import { generateKeyPairSync } from 'crypto';
import { CryptoManager, SecurityViolationError } from './crypto';

describe('CryptoManager', () => {
  let publicKeyPem: string;
  let privateKeyPem: string;
  const encryptionKey = 'super-secret-encryption-key-for-aes-256';

  beforeAll(() => {
    const { publicKey, privateKey } = generateKeyPairSync('ed25519', {
      publicKeyEncoding: { type: 'spki', format: 'pem' },
      privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
    });
    publicKeyPem = publicKey as string;
    privateKeyPem = privateKey as string;

    (CryptoManager as any).instance = undefined;
    CryptoManager.initialize(encryptionKey, publicKeyPem, privateKeyPem);
  });

  it('should encrypt and decrypt payloads successfully', async () => {
    const manager = CryptoManager.getInstance();
    const plaintext = 'sensitive payload data';

    const encrypted = await manager.encryptPayload(plaintext);
    expect(encrypted.ciphertext).toBeDefined();
    expect(encrypted.iv).toBeDefined();
    expect(encrypted.authTag).toBeDefined();

    const decrypted = await manager.decryptPayload(encrypted);
    expect(decrypted).toBe(plaintext);
  });

  it('should throw SecurityViolationError if auth tag is tampered', async () => {
    const manager = CryptoManager.getInstance();
    const plaintext = 'sensitive payload data';

    const encrypted = await manager.encryptPayload(plaintext);
    
    // Tamper the tag
    encrypted.authTag = '00'.repeat(16);

    await expect(manager.decryptPayload(encrypted)).rejects.toThrow(SecurityViolationError);
  });

  it('should sign and verify events successfully', async () => {
    const manager = CryptoManager.getInstance();
    const data = JSON.stringify({ event: 'test' });
    
    const signature = await manager.signEvent(data);
    expect(signature).toBeDefined();

    const isValid = await manager.verifyEvent(data, signature);
    expect(isValid).toBe(true);
  });

  it('should reject invalid event signatures', async () => {
    const manager = CryptoManager.getInstance();
    const data = JSON.stringify({ event: 'test' });
    
    let signature = await manager.signEvent(data);
    
    // Reverse signature to guarantee tampering
    signature = signature.split('').reverse().join('');
    
    const isValid = await manager.verifyEvent(data, signature);
    expect(isValid).toBe(false);
  });
});
