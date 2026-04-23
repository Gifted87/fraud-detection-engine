import {
  createCipheriv,
  createDecipheriv,
  createSign,
  createVerify,
  randomBytes,
  scryptSync,
  KeyObject,
  createPublicKey,
  createPrivateKey,
  sign,
  verify,
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

  private constructor(encryptionKey: string, salt: string, publicKeyPem: string, privateKeyPem: string) {
    // Derive a 32-byte key for AES-256 using a deployment-specific, non-static salt.
    // The salt MUST be provided via CRYPTO_SALT env var — a hardcoded literal defeats the
    // purpose of salting and enables precomputed rainbow-table attacks.
    this.aesKey = scryptSync(encryptionKey, salt, 32);
    this.signingPublicKey = createPublicKey({
      key: publicKeyPem,
      format: 'pem',
      type: 'spki',
    });
    this.signingPrivateKey = createPrivateKey({
      key: privateKeyPem,
      format: 'pem',
      type: 'pkcs8',
    });
  }

  /**
   * Initializes the CryptoManager singleton.
   * 
   * @param encryptionKey  Raw encryption key material (from env var).
   * @param salt           Deployment-specific salt string (from CRYPTO_SALT env var).
   *                       Must be at least 16 bytes when hex-decoded or treated as a string.
   *                       MUST NOT be a hardcoded literal.
   * @param publicKeyPem   PEM-encoded Ed25519 public key.
   * @param privateKeyPem  PEM-encoded Ed25519 private key.
   */
  public static initialize(encryptionKey: string, salt: string, publicKeyPem: string, privateKeyPem: string): void {
    if (!CryptoManager.instance) {
      if (!salt || salt.length < 8) {
        throw new ProcessingError('CRYPTO_SALT must be at least 8 characters. A static or empty salt defeats key derivation security.');
      }
      CryptoManager.instance = new CryptoManager(encryptionKey, salt, publicKeyPem, privateKeyPem);
    }
  }

  public static getInstance(): CryptoManager {
    if (!CryptoManager.instance) {
      throw new Error('CryptoManager not initialized. Call initialize() first.');
    }
    return CryptoManager.instance;
  }

  /**
   * Encrypts plaintext using AES-256-GCM with a random IV per call.
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

  public async signEvent(data: string): Promise<string> {
    // For Ed25519, we use the direct sign function.
    // The algorithm is set to null because Ed25519 handles hashing internally.
    const signature = sign(null, Buffer.from(data), this.signingPrivateKey);
    return signature.toString('hex');
  }

  public async verifyEvent(data: string, signature: string): Promise<boolean> {
    try {
      return verify(null, Buffer.from(data), this.signingPublicKey, Buffer.from(signature, 'hex'));
    } catch (err) {
      return false;
    }
  }
}
