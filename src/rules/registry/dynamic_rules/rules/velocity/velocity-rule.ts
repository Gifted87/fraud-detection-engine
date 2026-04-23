import { Histogram, Registry } from 'prom-client';
import { FraudRule, RuleEvaluationResult } from '../../contracts/fraud-rule-contract';
import { Transaction } from '../../../../../core/domain_models/definitions/transaction.interface';
import { ProjectionStore } from '../../../../../store/projection_store/projection-store';
import { Logger } from 'pino';
import { SystemConfiguration } from '../../../../../core/domain_models/dependency_config';

interface Dependencies {
  projectionStore: ProjectionStore;
  registry: Registry;
  logger: Logger;
  config: SystemConfiguration;
}

/**
 * VelocityRule
 * 
 * Identifies high-frequency transaction patterns for a specific user within a defined sliding window.
 * If the transaction count exceeds the configured threshold, the rule marks the transaction as suspicious.
 *
 * Configuration is hot-reloadable at runtime via `reloadConfig()`.  This allows the operations team to
 * update `VELOCITY_THRESHOLD` and `VELOCITY_WINDOW_SECONDS` environment variables and trigger a
 * `SIGHUP` signal without restarting the process or dropping Kafka consumer connections.
 */
export class VelocityRule implements FraudRule {
  public readonly ruleId: string = 'velocity-rule-v1';
  public readonly description: string = 'Detects high-frequency transactions exceeding threshold within a sliding window.';

  private threshold: number;
  private windowSizeSeconds: number;
  private readonly metrics: Histogram<string>;
  private readonly logger: Logger;
  private readonly registry: Registry;
  private readonly projectionStore: ProjectionStore;
  private readonly environment: 'development' | 'production' | 'test';

  constructor({ projectionStore, registry, logger, config }: Dependencies) {
    this.projectionStore = projectionStore;
    this.registry = registry;
    this.logger = logger;
    this.environment = config.NODE_ENV;
    this.threshold = config.VELOCITY_THRESHOLD;
    this.windowSizeSeconds = config.VELOCITY_WINDOW_SECONDS;

    this.metrics = new Histogram({
      name: 'fraud_engine_rule_velocity_latency_seconds',
      help: 'Latency of velocity rule evaluation',
      registers: [this.registry],
      labelNames: ['ruleId', 'environment'],
      buckets: [0.001, 0.002, 0.005, 0.01, 0.025, 0.05],
    });
  }

  /**
   * Reloads rule configuration from the centralized config object.
   */
  public reloadConfig(config: SystemConfiguration): void {
    const newThreshold = config.VELOCITY_THRESHOLD;
    const newWindow = config.VELOCITY_WINDOW_SECONDS;

    if (newThreshold !== this.threshold || newWindow !== this.windowSizeSeconds) {
      this.logger.info({ 
        ruleId: this.ruleId, 
        threshold: newThreshold, 
        windowSizeSeconds: newWindow 
      }, 'VelocityRule config reloaded');
      this.threshold = newThreshold;
      this.windowSizeSeconds = newWindow;
    }
  }

  /**
   * Evaluates if a transaction is suspicious based on user velocity.
   */
  public async evaluate(transaction: Transaction): Promise<RuleEvaluationResult> {
    const start = process.hrtime.bigint();

    try {
      // Fetch current transaction count for the user from ProjectionStore within the sliding window
      const count = await this.projectionStore.getTransactionCount(transaction.userId, this.windowSizeSeconds);

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

      this.logger.info({
        ruleId: this.ruleId,
        transactionId: transaction.transactionId,
        userId: transaction.userId,
        calculatedCount: count,
        isSuspicious: result.isSuspicious,
      }, 'VelocityRule evaluated');

      return result;

    } catch (error) {
      this.logger.fatal({
        ruleId: this.ruleId,
        transactionId: transaction.transactionId,
        error: error instanceof Error ? error.message : 'Unknown error'
      }, 'VelocityRule evaluation failed');
      throw error;
    } finally {
      const end = process.hrtime.bigint();
      const latency = Number(end - start) / 1e9;
      this.metrics.labels(this.ruleId, this.environment).observe(latency);
    }
  }
}
