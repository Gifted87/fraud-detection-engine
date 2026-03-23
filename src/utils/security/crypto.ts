import {
  createCipheriv,
  createDecipheriv,
  createSign,
  createVerify,
  randomBytes,
  scryptSync,
  KeyObject,
} from 'crypto';

/**
 * Custom Error classes to distinguish between processing errors and security violations.
 */
export class SecurityViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SecurityViolationError';
  }
}

export class ProcessingError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ProcessingError';
  }
}

/**
 * Interface for encryption results.
 */
export interface EncryptedPayload {
  ciphertext: string;
  iv: string;
  authTag: string;
}

/**
 * CryptoManager singleton to handle AES-256-GCM encryption and Ed25519 signing.
 */
export class CryptoManager {
  private static instance: CryptoManager;
  private readonly aesKey: Buffer;
  private readonly signingPublicKey: KeyObject;
  private readonly signingPrivateKey: KeyObject;

  private constructor(encryptionKey: string, publicKeyPem: string, privateKeyPem: string) {
    // Derive a 32-byte key for AES-256
    this.aesKey = scryptSync(encryptionKey, 'salt', 32);
    this.signingPublicKey = KeyObject.fromPublicKey({
      key: publicKeyPem,
      format: 'pem',
      type: 'spki',
    });
    this.signingPrivateKey = KeyObject.fromPrivateKey({
      key: privateKeyPem,
      format: 'pem',
      type: 'pkcs8',
    });
  }

  public static initialize(encryptionKey: string, publicKeyPem: string, privateKeyPem: string): void {
    if (!CryptoManager.instance) {
      CryptoManager.instance = new CryptoManager(encryptionKey, publicKeyPem, privateKeyPem);
    }
  }

  public static getInstance(): CryptoManager {
    if (!CryptoManager.instance) {
      throw new Error('CryptoManager not initialized');
    }
    return CryptoManager.instance;
  }

  /**
   * Encrypts plaintext using AES-256-GCM.
   */
  public async encryptPayload(plaintext: string): Promise<EncryptedPayload> {
    const iv = randomBytes(12);
    const cipher = createCipheriv('aes-256-gcm', this.aesKey, iv);

    const encrypted = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()]);
    const authTag = cipher.getAuthTag();

    return {
      ciphertext: encrypted.toString('hex'),
      iv: iv.toString('hex'),
      authTag: authTag.toString('hex'),
    };
  }

  /**
   * Decrypts ciphertext using AES-256-GCM.
   * Throws SecurityViolationError if authentication fails.
   */
  public async decryptPayload(payload: EncryptedPayload): Promise<string> {
    try {
      const decipher = createDecipheriv(
        'aes-256-gcm',
        this.aesKey,
        Buffer.from(payload.iv, 'hex')
      );
      decipher.setAuthTag(Buffer.from(payload.authTag, 'hex'));

      const decrypted = Buffer.concat([
        decipher.update(Buffer.from(payload.ciphertext, 'hex')),
        decipher.final(),
      ]);

      return decrypted.toString('utf8');
    } catch (err) {
      throw new SecurityViolationError('Decryption failed: integrity check failed or invalid data');
    }
  }

  /**
   * Signs a serialized JSON string using Ed25519.
   */
  public async signEvent(data: string): Promise<string> {
    const signer = createSign('sha256');
    signer.update(data);
    signer.end();
    return signer.sign(this.signingPrivateKey, 'hex');
  }

  /**
   * Verifies an Ed25519 signature.
   */
  public async verifyEvent(data: string, signature: string): Promise<boolean> {
    try {
      const verifier = createVerify('sha256');
      verifier.update(data);
      verifier.end();
      return verifier.verify(this.signingPublicKey, signature, 'hex');
    } catch (err) {
      return false;
    }
  }
}
