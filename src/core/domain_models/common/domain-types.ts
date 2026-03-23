/**
 * @fileoverview Foundational domain types and branded identifiers for the fraud detection engine.
 * Implements strict type-safety and immutability for high-throughput financial transaction processing.
 */

/**
 * Nominal typing utility for creating branded primitives.
 * Adds a compile-time-only tag to prevent accidental assignment between different ID types.
 */
export type Branded<T, B> = T & { readonly __brand: B };

/**
 * Branded identifier for system users.
 */
export type UserId = Branded<string, 'UserId'>;

/**
 * Branded identifier for transactions.
 */
export type TransactionId = Branded<string, 'TransactionId'>;

/**
 * Branded identifier for merchants.
 */
export type MerchantId = Branded<string, 'MerchantId'>;

/**
 * Represents financial values using an integer-based 'MinorUnit' pattern to prevent floating-point errors.
 * E.g., USD 10.00 is represented as 1000.
 */
export interface MonetaryAmount {
  readonly value: bigint;
  readonly currency: string;
}

/**
 * Standard metadata for event-sourced domain objects, supporting versioning and security.
 */
export interface EventMetadata {
  readonly schemaVersion: number;
  readonly createdAt: number;
  readonly signature: string;
}

/**
 * Defines an immutable base for domain entities.
 */
export interface DomainEntity {
  readonly metadata: EventMetadata;
}

/**
 * Type guard for UserId.
 */
export const isUserId = (id: string): id is UserId => {
  return typeof id === 'string' && id.length > 0;
};

/**
 * Type guard for TransactionId.
 */
export const isTransactionId = (id: string): id is TransactionId => {
  return typeof id === 'string' && id.length > 0;
};

/**
 * Type guard for MerchantId.
 */
export const isMerchantId = (id: string): id is MerchantId => {
  return typeof id === 'string' && id.length > 0;
};

/**
 * Utility function to safely cast a raw string to a UserId.
 * Use only at the system boundary (e.g., ingress from Kafka/API).
 */
export const asUserId = (id: string): UserId => {
  if (!isUserId(id)) throw new Error('Invalid UserId format');
  return id;
};

/**
 * Utility function to safely cast a raw string to a TransactionId.
 * Use only at the system boundary (e.g., ingress from Kafka/API).
 */
export const asTransactionId = (id: string): TransactionId => {
  if (!isTransactionId(id)) throw new Error('Invalid TransactionId format');
  return id;
};

/**
 * Utility function to safely cast a raw string to a MerchantId.
 * Use only at the system boundary (e.g., ingress from Kafka/API).
 */
export const asMerchantId = (id: string): MerchantId => {
  if (!isMerchantId(id)) throw new Error('Invalid MerchantId format');
  return id;
};

/**
 * Creates a monetary amount object enforcing big integer representation.
 */
export const createMonetaryAmount = (value: bigint | number | string, currency: string): MonetaryAmount => {
  return Object.freeze({
    value: BigInt(value),
    currency,
  });
};
