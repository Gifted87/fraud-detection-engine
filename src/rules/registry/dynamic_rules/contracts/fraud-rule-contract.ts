import { z } from 'zod';
import { Transaction } from '../../../../core/domain_models/definitions/transaction.interface';

/**
 * Specification for the result of a fraud rule evaluation.
 * Enforces strict risk scoring and reason reporting.
 */
export interface RuleEvaluationResult {
  readonly isSuspicious: boolean;
  readonly riskScore: number;
  readonly reason: string;
  readonly metadata?: Record<string, unknown>;
}

/**
 * Zod schema for runtime validation of RuleEvaluationResult.
 * Ensures that all rule implementations adhere to the contract boundaries.
 */
export const RuleEvaluationResultSchema = z.object({
  isSuspicious: z.boolean(),
  riskScore: z.number().min(0.0).max(1.0),
  reason: z.string().min(1, 'Reason must be a non-empty string'),
  metadata: z.record(z.unknown()).optional(),
});

/**
 * FraudRule interface defining the mandatory contract for all analytical 
 * fraud detection rules in the system.
 * 
 * Rules are designed to be stateless and asynchronous, facilitating
 * concurrent execution by the engine orchestrator.
 */
export interface FraudRule {
  /** Unique identifier for the rule (e.g., 'velocity-check-v1'). */
  readonly ruleId: string;

  /** Human-readable description of the rule's analytical purpose. */
  readonly description: string;

  /**
   * Asynchronous evaluation of a transaction event.
   * Rules must be non-blocking and stateless, querying the ProjectionStore
   * for any necessary historical state.
   * 
   * @param transaction The immutable transaction event to evaluate.
   * @returns A Promise resolving to a validated RuleEvaluationResult.
   */
  evaluate(transaction: Transaction): Promise<RuleEvaluationResult>;
}

/**
 * Error class used by the orchestrator to wrap rule-specific failures.
 */
export class RuleEvaluationError extends Error {
  constructor(
    public readonly ruleId: string,
    public readonly originalError: unknown
  ) {
    super(`Rule evaluation failed for ${ruleId}: ${originalError instanceof Error ? originalError.message : 'Unknown error'}`);
    this.name = 'RuleEvaluationError';
  }
}
