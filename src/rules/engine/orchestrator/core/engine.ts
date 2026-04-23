/**
 * @fileoverview Engine Core Orchestrator Implementation.
 * Primary decision-making hub for the real-time fraud detection engine.
 * Orchestrates rule execution, risk aggregation, and flagging events.
 */

import {
  Transaction,
  isTransactionValidated,
} from '../../../../core/domain_models/definitions/transaction.interface';
import { RuleRegistry } from '../../../registry/dynamic_rules/registry/rule-registry';
import { AlertingSubsystem } from '../alerts/alerts';
import { RiskAggregator } from '../contracts/engine-contracts';
import { OrchestrationMetricsCollector } from '../metrics/metrics';
import { RiskScore, RuleResult, OrchestrationFailure, asRiskScore } from '../contracts/engine-contracts';
import { SystemConfiguration } from '../../../../core/domain_models/dependency_config';
import { Logger } from 'pino';

interface Dependencies {
  ruleRegistry: RuleRegistry;
  riskAggregator: RiskAggregator;
  alertingSubsystem: AlertingSubsystem;
  orchestrationMetrics: OrchestrationMetricsCollector;
  config: SystemConfiguration;
  logger: Logger;
}

/**
 * Orchestrator configuration
 */
export interface OrchestratorConfig {
  readonly fraudThreshold: number;
  readonly environment: 'development' | 'production' | 'test';
  /**
   * Rule IDs that are considered mission-critical.
   * If any of these rules fail during evaluation, the orchestrator applies a
   * fail-CLOSED policy: the aggregated score is forced to 1.0 and the
   * transaction is flagged, rather than silently approving it.
   *
   * This prevents an attacker from exploiting infrastructure failures (e.g.,
   * a targeted DoS against Redis) to disable fraud detection.
   */
  readonly criticalRuleIds?: readonly string[];
}

export class EngineCoreOrchestrator {
  private readonly ruleRegistry: RuleRegistry;
  private readonly riskAggregator: RiskAggregator;
  private readonly alertSubsystem: AlertingSubsystem;
  private readonly metrics: OrchestrationMetricsCollector;
  private readonly config: {
    readonly fraudThreshold: number;
    readonly environment: 'development' | 'production' | 'test';
    readonly criticalRuleIds: readonly string[];
  };
  private readonly criticalRuleIdSet: ReadonlySet<string>;
  private readonly logger: Logger;

  constructor({
    ruleRegistry,
    riskAggregator,
    alertingSubsystem,
    orchestrationMetrics,
    config,
    logger
  }: Dependencies) {
    this.ruleRegistry = ruleRegistry;
    this.riskAggregator = riskAggregator;
    this.alertSubsystem = alertingSubsystem;
    this.metrics = orchestrationMetrics;
    this.config = {
      fraudThreshold: config.FRAUD_THRESHOLD,
      environment: config.NODE_ENV,
      criticalRuleIds: config.CRITICAL_RULE_IDS
    };
    this.logger = logger;
    this.criticalRuleIdSet = new Set(this.config.criticalRuleIds);
  }

  /**
   * Main entry point for transaction orchestration.
   * Processes a transaction through concurrent rule evaluation,
   * risk aggregation, and conditional alerting.
   */
  public async orchestrate(transaction: Transaction): Promise<void> {
    const startNs = process.hrtime.bigint();

    try {
      // 1. Initial Validation
      if (!isTransactionValidated(transaction)) {
        throw new Error('Only validated transactions can be orchestrated');
      }

      // 2. Concurrent Rule Evaluation (Promise.allSettled for reliability)
      const ruleResults = await this.executeRules(transaction);

      // 3. Fail-closed check: if any critical rule failed, force max risk score.
      //    Only RULE_EXECUTION_FAILURE variants carry a ruleId — narrow the union.
      const criticalFailure = ruleResults.failures.find(
        (f): f is Extract<typeof f, { type: 'RULE_EXECUTION_FAILURE' }> =>
          f.type === 'RULE_EXECUTION_FAILURE' && this.criticalRuleIdSet.has(f.ruleId)
      );

      if (criticalFailure) {
        this.logger.error({
          transactionId: transaction.transactionId,
          failedRuleId: criticalFailure.ruleId,
          error: criticalFailure.error,
        }, 'Critical rule failure — enforcing fail-closed policy, transaction will be flagged');

        // Force-flag the transaction at maximum risk
        await this.handleFlagging(transaction, asRiskScore(1.0), ruleResults.successful);
        return;
      }

      // 4. Risk Aggregation (from successfully-evaluated rules only)
      const aggregatedScore = this.riskAggregator.aggregate(ruleResults.successful);

      // 5. Threshold Comparison and Alerting
      if (aggregatedScore >= this.config.fraudThreshold) {
        await this.handleFlagging(transaction, aggregatedScore, ruleResults.successful);
      }

    } catch (error) {
      this.metrics.incrementErrorCount(this.config.environment, 'INTERNAL_SYSTEM_ERROR');
      this.logger.fatal({
        transactionId: transaction.transactionId,
        error: error instanceof Error ? error.message : 'Unknown error',
      }, 'Orchestration pipeline failure');
      throw error;
    } finally {
      this.metrics.observeEndToEndLatency(this.config.environment, startNs);
    }
  }

  /**
   * Executes all rules concurrently using Promise.allSettled for strict fault isolation.
   * Failures are collected separately from successes so the caller can apply
   * the appropriate fail-closed or fail-open policy per rule criticality.
   */
  private async executeRules(
    transaction: Transaction
  ): Promise<{ successful: RuleResult[]; failures: OrchestrationFailure[] }> {
    const rules = this.ruleRegistry.getRules();

    const results = await Promise.allSettled(
      rules.map((rule) => rule.evaluate(transaction))
    );

    const successful: RuleResult[] = [];
    const failures: OrchestrationFailure[] = [];

    results.forEach((res, index) => {
      const rule = rules[index];
      if (res.status === 'fulfilled') {
        successful.push({
          ruleId: rule.ruleId,
          score: asRiskScore(res.value.riskScore),
          isSuspicious: res.value.isSuspicious,
          findings: res.value.reason,
          timestamp: BigInt(Date.now()),
        });
      } else {
        failures.push({
          type: 'RULE_EXECUTION_FAILURE',
          ruleId: rule.ruleId,
          error: res.reason instanceof Error ? res.reason.message : 'Unknown',
        });
        this.metrics.incrementErrorCount(
          this.config.environment,
          'RULE_EXECUTION_FAILURE',
          rule.ruleId
        );
      }
    });

    return { successful, failures };
  }

  /**
   * Dispatches high-priority alerting event.
   */
  private async handleFlagging(
    transaction: Transaction,
    riskScore: RiskScore,
    ruleResults: RuleResult[]
  ): Promise<void> {
    const reasons = ruleResults
      .filter((r) => r.isSuspicious)
      .map((r) => r.findings)
      .join(' | ');

    await this.alertSubsystem.dispatchFlag(
      transaction.transactionId,
      transaction.userId,
      transaction.merchantId,
      transaction.amount,
      transaction.telemetry,
      reasons,
      riskScore,
      'v1.0.0'
    );
  }
}
