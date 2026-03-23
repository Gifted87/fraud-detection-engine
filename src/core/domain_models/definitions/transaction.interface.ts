/**
 * @fileoverview Immutable domain model definitions for transaction events.
 * This file contains the structural contracts for the Event Sourced architecture,
 * ensuring type safety, immutability, and performance for high-throughput fraud detection.
 */

import {
  UserId,
  TransactionId,
  MerchantId,
  MonetaryAmount,
  DomainEntity,
} from '../common/domain-types';

/**
 * High-fidelity environmental telemetry for fraud scoring.
 */
export interface Telemetry {
  readonly latitude: number;
  readonly longitude: number;
  readonly deviceFingerprint: string;
  readonly ipAddress: string;
  readonly userAgent: string;
}

/**
 * Base transaction event interface.
 */
export interface TransactionEvent extends DomainEntity {
  readonly transactionId: TransactionId;
  readonly userId: UserId;
  readonly merchantId: MerchantId;
  readonly amount: MonetaryAmount;
  readonly timestamp: bigint;
  readonly telemetry: Telemetry;
}

/**
 * Initial state of a transaction upon ingestion.
 */
export interface TransactionInitiated extends TransactionEvent {
  readonly type: 'TransactionInitiated';
}

/**
 * State after successful cryptographic validation.
 */
export interface TransactionValidated extends TransactionEvent {
  readonly type: 'TransactionValidated';
  readonly validatorId: string;
}

/**
 * Terminal state for flagged fraudulent transactions.
 */
export interface TransactionFlagged extends TransactionEvent {
  readonly type: 'TransactionFlagged';
  readonly reason: string;
  readonly riskScore: number;
}

/**
 * Union type representing all possible transaction event states in the system.
 */
export type Transaction = 
  | TransactionInitiated 
  | TransactionValidated 
  | TransactionFlagged;

/**
 * Type guard for TransactionInitiated.
 */
export const isTransactionInitiated = (event: Transaction): event is TransactionInitiated =>
  event.type === 'TransactionInitiated';

/**
 * Type guard for TransactionValidated.
 */
export const isTransactionValidated = (event: Transaction): event is TransactionValidated =>
  event.type === 'TransactionValidated';

/**
 * Type guard for TransactionFlagged.
 */
export const isTransactionFlagged = (event: Transaction): event is TransactionFlagged =>
  event.type === 'TransactionFlagged';
