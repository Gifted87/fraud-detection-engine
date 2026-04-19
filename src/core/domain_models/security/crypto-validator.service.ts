import { createHmac, timingSafeEqual } from 'crypto';

/**
 * Custom error thrown when the CryptoValidator service is accessed before initialization.
 */
export class InitializationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InitializationError';
  }
}

/**
 * Custom error thrown when an operational error occurs during cryptographic processing.
 */
export class CryptographicOperationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CryptographicOperationError';
  }
}

/**
 * CryptoValidator service responsible for HMAC-SHA256 signing and verification.
 * Designed for high-throughput event processing with timing-safe verification.
 */
export class CryptoValidator {
  private static instance: CryptoValidator | null = null;
  private readonly secretKey: Buffer;

  private constructor(signingKey: string) {
    this.secretKey = Buffer.from(signingKey, 'utf8');
  }

  /**
   * Initializes the singleton instance of the CryptoValidator.
   * Should be called at system boot time.
   * 
   * @param signingKey The secret key used for HMAC signing.
   */
  public static initialize(signingKey: string): void {
    if (!signingKey || signingKey.length === 0) {
      throw new InitializationError('Signing key cannot be empty.');
    }
    if (!CryptoValidator.instance) {
      CryptoValidator.instance = new CryptoValidator(signingKey);
    }
  }

  /**
   * Returns the initialized singleton instance of the CryptoValidator.
   */
  public static getInstance(): CryptoValidator {
    if (!CryptoValidator.instance) {
      throw new InitializationError('CryptoValidator has not been initialized. Call initialize() first.');
    }
    return CryptoValidator.instance;
  }

  /**
   * Signs a data payload using HMAC-SHA256.
   * 
   * @param data The JSON-stringified payload to sign.
   * @returns A hex-encoded HMAC signature string.
   */
  public async sign(data: string): Promise<string> {
    try {
      const hmac = createHmac('sha256', this.secretKey);
      hmac.update(data);
      return hmac.digest('hex');
    } catch (err) {
      throw new CryptographicOperationError('Failed to sign the payload.');
    }
  }

  /**
   * Verifies the authenticity of a payload against a provided signature.
   * Uses timing-safe comparison to prevent side-channel attacks.
   * 
   * @param data The original JSON-stringified payload.
   * @param signature The hex-encoded signature to verify.
   * @returns True if the signature is valid, false otherwise.
   */
  public async verify(data: string, signature: string): Promise<boolean> {
    try {
      const hmac = createHmac('sha256', this.secretKey);
      hmac.update(data);
      const calculatedSignature = hmac.digest();
      const providedSignatureBuffer = Buffer.from(signature, 'hex');

      // Verify the buffers have the same length before comparison to ensure safety
      if (calculatedSignature.length !== providedSignatureBuffer.length) {
        return false;
      }

      return timingSafeEqual(calculatedSignature, providedSignatureBuffer);
    } catch (err) {
      // In case of error (e.g. invalid encoding), return false to signify verification failure
      return false;
    }
  }
}
