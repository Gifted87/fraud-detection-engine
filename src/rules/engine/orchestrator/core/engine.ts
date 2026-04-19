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
import { RiskScore, RuleResult, OrchestrationFailure } from '../contracts/engine-contracts';

/**
 * Orchestrator configuration
 */
export interface OrchestratorConfig {
  readonly fraudThreshold: number;
  readonly environment: 'development' | 'production' | 'test';
}

/**
 * Engine Core Orchestrator
 * Adheres to non-blocking event-loop architecture.
 */
export class EngineCoreOrchestrator {
  private readonly ruleRegistry: RuleRegistry;
  private readonly riskAggregator: RiskAggregator;
  private readonly alertSubsystem: AlertingSubsystem;
  private readonly metrics: OrchestrationMetricsCollector;
  private readonly config: OrchestratorConfig;

  constructor(
    ruleRegistry: RuleRegistry,
    riskAggregator: RiskAggregator,
    alertSubsystem: AlertingSubsystem,
    metrics: OrchestrationMetricsCollector,
    config: OrchestratorConfig
  ) {
    this.ruleRegistry = ruleRegistry;
    this.riskAggregator = riskAggregator;
    this.alertSubsystem = alertSubsystem;
    this.metrics = metrics;
    this.config = config;
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
      // Note: RuleRegistry is assumed to be the interface to dynamic rules
      const ruleResults = await this.executeRules(transaction);

      // 3. Risk Aggregation
      const aggregatedScore = this.riskAggregator.aggregate(ruleResults.successful);

      // 4. Threshold Comparison and Alerting
      if (aggregatedScore >= this.config.fraudThreshold) {
        await this.handleFlagging(transaction, aggregatedScore, ruleResults.successful);
      }

    } catch (error) {
      this.metrics.incrementErrorCount(this.config.environment, 'INTERNAL_SYSTEM_ERROR');
      console.error(JSON.stringify({
        level: 'critical',
        message: 'Orchestration pipeline failure',
        transactionId: transaction.transactionId,
        error: error instanceof Error ? error.message : 'Unknown error',
        timestamp: Date.now()
      }));
    } finally {
      this.metrics.observeEndToEndLatency(this.config.environment, startNs);
    }
  }

  /**
   * Executes all rules concurrently with strict fault tolerance.
   */
  private async executeRules(transaction: Transaction): Promise<{ successful: RuleResult[], failures: OrchestrationFailure[] }> {
    // Assuming RuleRegistry has an interface to get list of rules
    // Using Promise.allSettled as per requirement 3.2
    const registry = this.ruleRegistry as any; // Dynamic access if needed
    const rules = registry.rules ? Array.from((registry.rules as Map<string, any>).values()) : [];
    
    const results = await Promise.allSettled(rules.map((rule: any) => rule.evaluate(transaction)));
    
    const successful: RuleResult[] = [];
    const failures: OrchestrationFailure[] = [];

    results.forEach((res, index) => {
      const rule = rules[index];
      if (res.status === 'fulfilled') {
        successful.push({
          ruleId: rule.ruleId,
          score: res.value.riskScore,
          isSuspicious: res.value.isSuspicious,
          findings: res.value.reason,
          timestamp: BigInt(Date.now())
        });
      } else {
        failures.push({
          type: 'RULE_EXECUTION_FAILURE',
          ruleId: rule.ruleId,
          error: res.reason instanceof Error ? res.reason.message : 'Unknown'
        });
        this.metrics.incrementErrorCount(this.config.environment, 'RULE_EXECUTION_FAILURE', rule.ruleId);
      }
    });

    return { successful, failures };
  }

  /**
   * Dispatches high-priority alerting event
   */
  private async handleFlagging(
    transaction: Transaction,
    riskScore: RiskScore,
    ruleResults: RuleResult[]
  ): Promise<void> {
    const reasons = ruleResults.filter(r => r.isSuspicious).map(r => r.findings).join(' | ');
    
    await this.alertSubsystem.dispatchFlag(
      transaction.transactionId,
      transaction.userId,
      transaction.merchantId,
      transaction.amount,
      transaction.telemetry,
      reasons,
      riskScore,
      'v1.0.0' // Versioning
    );
  }
}
