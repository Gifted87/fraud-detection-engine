import { Histogram, Registry } from 'prom-client';
import { FraudRule, RuleEvaluationResult } from '../../contracts/fraud_rule.contract';
import { Transaction } from '../../../../../core/domain_models/definitions/transaction.interface';
import { ProjectionStore } from '../../../../../store/projection_store/projection-store';

/**
 * VelocityRule
 * 
 * Identifies high-frequency transaction patterns for a specific user within a defined sliding window.
 * If the transaction count exceeds the configured threshold, the rule marks the transaction as suspicious.
 */
export class VelocityRule implements FraudRule {
  public readonly ruleId: string = 'velocity-rule-v1';
  public readonly description: string = 'Detects high-frequency transactions exceeding threshold within a sliding window.';

  private readonly threshold: number;
  private readonly windowSizeSeconds: number;
  private readonly metrics: Histogram<string>;

  constructor(
    private readonly projectionStore: ProjectionStore,
    private readonly registry: Registry
  ) {
    // Dynamic configuration loaded from environment
    this.threshold = parseInt(process.env.VELOCITY_THRESHOLD || '10', 10);
    this.windowSizeSeconds = parseInt(process.env.VELOCITY_WINDOW_SECONDS || '60', 10);

    this.metrics = new Histogram({
      name: 'fraud_engine_rule_velocity_latency_seconds',
      help: 'Latency of velocity rule evaluation',
      registers: [this.registry],
      labelNames: ['ruleId', 'environment'],
      buckets: [0.001, 0.002, 0.005, 0.01, 0.025, 0.05],
    });
  }

  /**
   * Evaluates if a transaction is suspicious based on user velocity.
   */
  public async evaluate(transaction: Transaction): Promise<RuleEvaluationResult> {
    const start = process.hrtime.bigint();
    const environment = process.env.NODE_ENV || 'production';

    try {
      // 1. Fetch current transaction count for the user from ProjectionStore
      const count = await this.projectionStore.getTransactionCount(transaction.userId, this.windowSizeSeconds);

      // 2. Evaluate against threshold
      const isSuspicious = count > this.threshold;
      
      const result: RuleEvaluationResult = Object.freeze({
        isSuspicious,
        riskScore: isSuspicious ? 0.8 : 0.0,
        reason: isSuspicious 
          ? `User ${transaction.userId} exceeded velocity threshold of ${this.threshold} in ${this.windowSizeSeconds}s (count: ${count})`
          : 'Normal transaction velocity',
        metadata: {
          calculatedCount: count,
          threshold: this.threshold,
          windowSizeSeconds: this.windowSizeSeconds
        }
      });

      // 3. Structured Logging
      console.log(JSON.stringify({
        ruleId: this.ruleId,
        transactionId: transaction.transactionId,
        userId: transaction.userId,
        calculatedCount: count,
        isSuspicious: result.isSuspicious,
        timestamp: Date.now()
      }));

      return result;

    } catch (error) {
      // 4. Defensive fallback on error to prevent pipeline blockage
      console.error(JSON.stringify({
        level: 'critical',
        message: 'VelocityRule evaluation failed',
        ruleId: this.ruleId,
        transactionId: transaction.transactionId,
        error: error instanceof Error ? error.message : 'Unknown error'
      }));

      return Object.freeze({
        isSuspicious: false,
        riskScore: 0.0,
        reason: 'Rule execution error, defaulting to safe'
      });
    } finally {
      // 5. Instrumentation
      const end = process.hrtime.bigint();
      const latency = Number(end - start) / 1e9;
      this.metrics.labels(this.ruleId, environment).observe(latency);
    }
  }
}
