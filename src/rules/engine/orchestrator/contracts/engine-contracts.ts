/**
 * @fileoverview Fraud Detection Engine Orchestrator Contracts.
 * Defines the foundational types and interfaces for risk aggregation,
 * alerting, and performance observability.
 */

import { TransactionId, UserId, MerchantId, MonetaryAmount, Telemetry } from '../../../../core/domain_models/common/domain-types';

/**
 * A branded type for numerical risk scores, providing compile-time safety
 * and semantic clarity. Represents a danger level in the range [0.0, 1.0].
 */
export type RiskScore = number & { readonly __brand: 'RiskScore' };

/**
 * Branded identifier generator for risk scores.
 * Enforces the [0.0, 1.0] constraint at runtime.
 */
export const asRiskScore = (score: number): RiskScore => {
  if (score < 0.0 || score > 1.0) {
    throw new Error(`RiskScore must be in the range [0.0, 1.0]. Received: ${score}`);
  }
  return score as RiskScore;
};

/**
 * Result of an individual rule execution within the orchestrator.
 */
export interface RuleResult {
  readonly ruleId: string;
  readonly score: RiskScore;
  readonly isSuspicious: boolean;
  readonly findings: string;
  readonly timestamp: bigint;
}

/**
 * Union type for potential failures in the orchestration process.
 */
export type OrchestrationFailure = 
  | { readonly type: 'RULE_EXECUTION_FAILURE'; readonly ruleId: string; readonly error: string }
  | { readonly type: 'AGGREGATION_TIMEOUT'; readonly durationMs: number }
  | { readonly type: 'INTERNAL_SYSTEM_ERROR'; readonly message: string };

/**
 * Aggregated result of concurrent fraud rule evaluation.
 */
export interface AggregatedEvaluationResult {
  readonly transactionId: TransactionId;
  readonly aggregateScore: RiskScore;
  readonly isSuspicious: boolean;
  readonly ruleResults: ReadonlyArray<RuleResult>;
  readonly failures: ReadonlyArray<OrchestrationFailure>;
}

/**
 * Functional interface for the RiskAggregator.
 * Responsible for computing a weighted aggregate risk score from multiple rule evaluations.
 */
export interface RiskAggregator {
  /**
   * Computes the aggregated risk score based on individual rule results.
   */
  aggregate(results: ReadonlyArray<RuleResult>): RiskScore;
}

/**
 * Finalized flagging decision event for downstream processing.
 */
export interface AlertEvent {
  readonly idempotencyKey: string;
  readonly transactionId: TransactionId;
  readonly userId: UserId;
  readonly merchantId: MerchantId;
  readonly amount: MonetaryAmount;
  readonly riskScore: RiskScore;
  readonly violations: ReadonlyArray<string>;
  readonly telemetry: Telemetry;
  readonly orchestrationVersion: string;
  readonly createdAtNs: bigint;
}

/**
 * Captures high-precision timing for the orchestration pipeline.
 */
export interface LatencySnapshot {
  readonly startNs: bigint;
  readonly endNs: bigint;
  readonly durationNs: bigint;
}

/**
 * Telemetry for an individual rule execution.
 */
export interface RuleEvaluationMetrics {
  readonly ruleId: string;
  readonly startNs: bigint;
  readonly endNs: bigint;
  readonly success: boolean;
  readonly errorMessage?: string;
}

/**
 * Comprehensive metrics structure for orchestrator observability.
 */
export interface OrchestrationMetrics {
  readonly transactionId: TransactionId;
  readonly totalLatency: LatencySnapshot;
  readonly ruleMetrics: ReadonlyArray<RuleEvaluationMetrics>;
  readonly environment: 'development' | 'production' | 'test';
}
