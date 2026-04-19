/**
 * @fileoverview Event Envelope implementation for Kafka-based messaging.
 * Provides a secure, versioned, and type-safe wrapper for domain events.
 */

import { CryptoValidator } from '../security/crypto-validator.service';
import { Transaction } from '../definitions/transaction.interface';

/**
 * Custom error thrown when a security violation occurs during envelope processing.
 */
export class SecurityViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SecurityViolationError';
  }
}

/**
 * Metadata encapsulated within every message envelope.
 */
export interface EnvelopeMetadata {
  readonly schemaVersion: string;
  readonly createdAtNs: bigint;
  readonly provenanceTrace: string;
}

/**
 * Generic Message Envelope structure for Kafka events.
 */
export interface MessageEnvelope<T> {
  readonly metadata: EnvelopeMetadata;
  readonly payload: T;
  readonly signature: string;
}

/**
 * Factory class for creating and signing event envelopes.
 */
export class EventEnvelopeFactory {
  private static readonly SCHEMA_VERSION = 'v1.0';

  /**
   * Creates a signed MessageEnvelope for a given transaction event.
   * 
   * @param payload The domain event object.
   * @returns A signed MessageEnvelope.
   */
  public static async create<T extends Transaction>(payload: T): Promise<MessageEnvelope<T>> {
    const metadata: EnvelopeMetadata = {
      schemaVersion: this.SCHEMA_VERSION,
      createdAtNs: BigInt(process.hrtime.bigint()),
      provenanceTrace: `proc:${process.pid}`,
    };

    const envelopePayload = {
      metadata,
      payload,
    };

    const serializedData = JSON.stringify(envelopePayload, (key, value) => 
      typeof value === 'bigint' ? value.toString() : value
    );
    
    let signature: string;
    try {
      signature = await CryptoValidator.getInstance().sign(serializedData);
    } catch (err) {
      throw new SecurityViolationError(`Failed to sign envelope: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }

    const envelope: MessageEnvelope<T> = {
      ...envelopePayload,
      signature,
    };

    return Object.freeze(envelope);
  }

  /**
   * Validates the integrity of an envelope by re-verifying the signature.
   * 
   * @param envelope The envelope to verify.
   * @returns True if valid, throws SecurityViolationError if invalid.
   */
  public static async verifyEnvelope<T extends Transaction>(envelope: MessageEnvelope<T>): Promise<boolean> {
    const { signature, ...dataToVerify } = envelope;
    
    const serializedData = JSON.stringify(dataToVerify, (key, value) => 
      typeof value === 'bigint' ? value.toString() : value
    );
    
    let isValid: boolean;
    try {
      isValid = await CryptoValidator.getInstance().verify(serializedData, signature);
    } catch (err) {
      throw new SecurityViolationError(`Error during verification process: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }

    if (!isValid) {
      throw new SecurityViolationError('Envelope signature verification failed: Payload integrity compromised.');
    }

    return true;
  }
}
