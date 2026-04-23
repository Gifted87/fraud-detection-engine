import { z } from 'zod';

/**
 * Zod schema for validated Branded identifiers.
 */
const BrandedString = (brand: string) =>
  z.string().min(1, `${brand} cannot be empty`);

/**
 * Zod schema for MonetaryAmount.
 */
export const MonetaryAmountSchema = z.object({
  value: z.coerce.bigint(),
  currency: z.string().length(3, 'Currency must be a 3-letter ISO code'),
});

/**
 * Zod schema for Telemetry.
 */
export const TelemetrySchema = z.object({
  latitude: z.number(),
  longitude: z.number(),
  deviceFingerprint: z.string().min(1),
  ipAddress: z.string().ip(),
  userAgent: z.string().min(1),
});

/**
 * Base schema for all transaction events.
 */
const TransactionEventBaseSchema = z.object({
  transactionId: BrandedString('TransactionId'),
  userId: BrandedString('UserId'),
  merchantId: BrandedString('MerchantId'),
  amount: MonetaryAmountSchema,
  timestamp: z.coerce.bigint(),
  telemetry: TelemetrySchema,
});

/**
 * Schema for TransactionInitiated.
 */
export const TransactionInitiatedSchema = TransactionEventBaseSchema.extend({
  type: z.literal('TransactionInitiated'),
});

/**
 * Schema for TransactionValidated.
 */
export const TransactionValidatedSchema = TransactionEventBaseSchema.extend({
  type: z.literal('TransactionValidated'),
  validatorId: z.string().min(1),
});

/**
 * Schema for TransactionFlagged.
 */
export const TransactionFlaggedSchema = TransactionEventBaseSchema.extend({
  type: z.literal('TransactionFlagged'),
  reason: z.string().min(1),
  riskScore: z.number().min(0).max(1),
});

/**
 * Discriminated union of all transaction event types.
 */
export const TransactionSchema = z.discriminatedUnion('type', [
  TransactionInitiatedSchema,
  TransactionValidatedSchema,
  TransactionFlaggedSchema,
]);

/**
 * Schema for EnvelopeMetadata.
 */
export const EnvelopeMetadataSchema = z.object({
  schemaVersion: z.string().min(1),
  createdAtNs: z.coerce.bigint(),
  provenanceTrace: z.string().min(1),
});

/**
 * Generic Schema factory for MessageEnvelope.
 */
export const MessageEnvelopeSchema = <T extends z.ZodTypeAny>(payloadSchema: T) =>
  z.object({
    metadata: EnvelopeMetadataSchema,
    payload: payloadSchema,
    signature: z.string().min(1, 'Signature cannot be empty'),
  });

/**
 * Type-safe inferred types.
 */
export type TransactionInitiatedPayload = z.infer<typeof TransactionInitiatedSchema>;
export type TransactionValidatedPayload = z.infer<typeof TransactionValidatedSchema>;
export type TransactionFlaggedPayload = z.infer<typeof TransactionFlaggedSchema>;
export type TransactionPayload = z.infer<typeof TransactionSchema>;
export type EnvelopeMetadata = z.infer<typeof EnvelopeMetadataSchema>;
export type MessageEnvelope<T> = z.infer<ReturnType<typeof MessageEnvelopeSchema<z.ZodType<T>>>>;
