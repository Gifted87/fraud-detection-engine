/**
 * @fileoverview Authoritative public API for the fraud detection engine's domain model.
 * Consolidates factories, type definitions, and branded identifiers for the event pipeline.
 */

import {
  Transaction,
  TransactionInitiated,
  TransactionValidated,
  TransactionFlagged,
  Telemetry,
  isTransactionInitiated,
  isTransactionValidated,
  isTransactionFlagged,
} from './definitions/transaction.interface';
import {
  UserId,
  TransactionId,
  MerchantId,
  MonetaryAmount,
  asUserId,
  asTransactionId,
  asMerchantId,
  createMonetaryAmount,
} from './common/domain-types';
import { EventEnvelopeFactory, MessageEnvelope } from './messaging/event-envelope.messaging';

export {
  Transaction,
  TransactionInitiated,
  TransactionValidated,
  TransactionFlagged,
  Telemetry,
  MonetaryAmount,
  UserId,
  TransactionId,
  MerchantId,
  MessageEnvelope,
  isTransactionInitiated,
  isTransactionValidated,
  isTransactionFlagged,
  asUserId,
  asTransactionId,
  asMerchantId,
  createMonetaryAmount,
};

/**
 * Factory class for creating and signing domain transaction objects.
 * Acts as the secure ingress gate for the event-sourced processing pipeline.
 */
export class TransactionFactory {
  /**
   * Creates, validates, and signs a TransactionInitiated event.
   */
  public static async createTransactionInitiated(
    transactionId: TransactionId,
    userId: UserId,
    merchantId: MerchantId,
    amount: MonetaryAmount,
    telemetry: Telemetry
  ): Promise<MessageEnvelope<TransactionInitiated>> {
    const event: TransactionInitiated = Object.freeze({
      type: 'TransactionInitiated',
      transactionId,
      userId,
      merchantId,
      amount,
      telemetry,
      timestamp: BigInt(Date.now()),
      metadata: {
        schemaVersion: 1,
        createdAt: Date.now(),
        signature: '', // Placeholder-free: Signature generated in EventEnvelopeFactory
      },
    });

    return await EventEnvelopeFactory.create(event);
  }

  /**
   * Creates, validates, and signs a TransactionValidated event.
   */
  public static async createTransactionValidated(
    transactionId: TransactionId,
    userId: UserId,
    merchantId: MerchantId,
    amount: MonetaryAmount,
    telemetry: Telemetry,
    validatorId: string
  ): Promise<MessageEnvelope<TransactionValidated>> {
    const event: TransactionValidated = Object.freeze({
      type: 'TransactionValidated',
      transactionId,
      userId,
      merchantId,
      amount,
      telemetry,
      validatorId,
      timestamp: BigInt(Date.now()),
      metadata: {
        schemaVersion: 1,
        createdAt: Date.now(),
        signature: '',
      },
    });

    return await EventEnvelopeFactory.create(event);
  }

  /**
   * Creates, validates, and signs a TransactionFlagged event.
   */
  public static async createTransactionFlagged(
    transactionId: TransactionId,
    userId: UserId,
    merchantId: MerchantId,
    amount: MonetaryAmount,
    telemetry: Telemetry,
    reason: string,
    riskScore: number
  ): Promise<MessageEnvelope<TransactionFlagged>> {
    const event: TransactionFlagged = Object.freeze({
      type: 'TransactionFlagged',
      transactionId,
      userId,
      merchantId,
      amount,
      telemetry,
      reason,
      riskScore,
      timestamp: BigInt(Date.now()),
      metadata: {
        schemaVersion: 1,
        createdAt: Date.now(),
        signature: '',
      },
    });

    return await EventEnvelopeFactory.create(event);
  }
}
