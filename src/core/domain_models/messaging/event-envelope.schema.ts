/**
 * @fileoverview Event Envelope implementation for Kafka-based messaging.
 * Provides a secure, versioned, and type-safe wrapper for domain events.
 */

import { CryptoManager } from '../../../utils/security/crypto';
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

// ---------------------------------------------------------------------------
// Canonical (deterministic) JSON serialization
// ---------------------------------------------------------------------------

/**
 * Produces a deterministic JSON string by recursively sorting object keys
 * before serialization.
 *
 * **Why this matters:** JavaScript does not guarantee insertion-order key
 * iteration across all runtimes and versions.  If a producer serializes
 * `{ b: 1, a: 2 }` and a consumer serializes `{ a: 2, b: 1 }`, native
 * `JSON.stringify` may produce different byte sequences, causing HMAC
 * signature verification to fail even though the payloads are semantically
 * identical.  Canonical serialization eliminates this class of false-positive
 * security violations.
 *
 * BigInt values are serialized as their decimal string representation to
 * remain wire-compatible with the existing BigInt reviver in the consumer.
 */
export function canonicalizeJson(value: unknown): string {
  if (value === null || value === undefined) {
    return JSON.stringify(value);
  }

  if (typeof value === 'bigint') {
    return value.toString();
  }

  if (value instanceof Date) {
    return JSON.stringify(value.toISOString());
  }

  if (Array.isArray(value)) {
    const items = value.map((item) => canonicalizeJson(item)).join(',');
    return `[${items}]`;
  }

  if (typeof value === 'object') {
    const sortedKeys = Object.keys(value as object).sort();
    const pairs = sortedKeys.map((key) => {
      const v = (value as Record<string, unknown>)[key];
      const serializedValue = typeof v === 'bigint'
        ? v.toString()
        : typeof v === 'object' && v !== null
          ? canonicalizeJson(v)
          : JSON.stringify(v);
      return `${JSON.stringify(key)}:${serializedValue}`;
    });
    return `{${pairs.join(',')}}`;
  }

  // Primitives (string, number, boolean)
  return JSON.stringify(value);
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

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

    // Use canonical JSON so the signature is reproducible across any runtime
    // regardless of object key insertion order.
    const serializedData = canonicalizeJson(envelopePayload);
    
    let signature: string;
    try {
      signature = await CryptoManager.getInstance().signEvent(serializedData);
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
    
    // Must use the same canonical serializer as create() so the byte sequence
    // being verified is identical to the one that was signed.
    const serializedData = canonicalizeJson(dataToVerify);
    
    let isValid: boolean;
    try {
      isValid = await CryptoManager.getInstance().verifyEvent(serializedData, signature);
    } catch (err) {
      throw new SecurityViolationError(`Error during verification process: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }

    if (!isValid) {
      throw new SecurityViolationError('Envelope signature verification failed: Payload integrity compromised.');
    }

    return true;
  }
}
