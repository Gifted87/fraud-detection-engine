import { Registry, Histogram } from 'prom-client';
import { FraudRule, RuleEvaluationResult } from '../contracts/fraud-rule-contract';
import { Transaction } from '../../../../core/domain_models/definitions/transaction.interface';
import { Logger } from 'pino';
import { SystemConfiguration } from '../../../../core/domain_models/dependency_config';

interface Dependencies {
  registry: Registry;
  logger: Logger;
  config: SystemConfiguration;
}

/**
 * Interface for rules that support hot-reloading their configuration.
 */
export interface HotReloadableRule {
  reloadConfig(): void;
}

/**
 * Registry for managing and orchestrating FraudRule evaluation.
 * Acts as a thread-safe singleton for high-concurrency event processing.
 */
export class RuleRegistry {
  private readonly rules: Map<string, FraudRule> = new Map();
  private threshold: number;
  private readonly registry: Registry;
  private readonly logger: Logger;
  private readonly evaluationLatency: Histogram<string>;
  private criticalRuleIds: Set<string>;

  constructor({ registry, logger, config }: Dependencies) {
    this.registry = registry;
    this.logger = logger;
    this.threshold = config.FRAUD_THRESHOLD;
    this.criticalRuleIds = new Set(config.CRITICAL_RULE_IDS);

    this.evaluationLatency = new Histogram({
      name: 'fraud_engine_registry_evaluation_latency_seconds',
      help: 'Aggregate latency of all rule evaluations',
      registers: [this.registry],
      labelNames: ['environment'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
    });
  }



  /**
   * Registers a new fraud rule with runtime validation.
   */
  public registerRule(rule: FraudRule): void {
    if (this.rules.has(rule.ruleId)) {
      throw new Error(`Rule with ID ${rule.ruleId} already registered.`);
    }

    if (!rule.evaluate || typeof rule.evaluate !== 'function') {
      throw new Error(`Invalid rule implementation: ${rule.ruleId} lacks evaluate method.`);
    }

    this.rules.set(rule.ruleId, rule);
  }

  /**
   * Returns a copy of the registered rules for concurrent orchestration.
   */
  public getRules(): FraudRule[] {
    return Array.from(this.rules.values());
  }

  /**
   * Hot-reloads configuration for all registered rules that implement `reloadConfig()`.
   */
  public reloadAll(config: SystemConfiguration): void {
    this.threshold = config.FRAUD_THRESHOLD;
    this.criticalRuleIds = new Set(config.CRITICAL_RULE_IDS);
    
    let reloaded = 0;
    for (const [ruleId, rule] of this.rules) {
      if (isHotReloadable(rule)) {
        try {
          rule.reloadConfig(config);
          reloaded++;
        } catch (err) {
          this.logger.error({ 
            ruleId, 
            error: err instanceof Error ? err.message : 'Unknown error' 
          }, 'Failed to reload config for rule');
        }
      }
    }
    this.logger.info({ 
      totalRules: this.rules.size, 
      reloadedRules: reloaded 
    }, 'Rule registry hot-reload complete');
  }

  /**
   * Orchestrates concurrent evaluation of all registered rules.
   *
   * Fail-CLOSED policy for critical rules:
   *   If any rule in the `criticalRuleIds` set fails during evaluation, the aggregate
   *   score is forced to 1.0 and the transaction is flagged.  This prevents an
   *   infrastructure outage (e.g., Redis down) from being exploited to silently
   *   approve fraudulent transactions.
   *
   * Fail-OPEN policy for non-critical rules:
   *   Failures are logged and treated as a score of 0.0 (current behavior preserved).
   */
  public async evaluateAll(
    transaction: Transaction
  ): Promise<{ isSuspicious: boolean; aggregateScore: number; results: RuleEvaluationResult[] }> {
    const start = process.hrtime.bigint();
    const environment = process.env.NODE_ENV || 'production';

    const ruleEntries = Array.from(this.rules.entries());

    const settledResults = await Promise.allSettled(
      ruleEntries.map(([, rule]) => rule.evaluate(transaction))
    );

    const results: RuleEvaluationResult[] = [];
    let criticalRuleFailed = false;
    let failedCriticalRuleId: string | undefined;

    settledResults.forEach((settled, index) => {
      const [ruleId, rule] = ruleEntries[index];
      if (settled.status === 'fulfilled') {
        results.push(settled.value);
      } else {
        const isCritical = this.criticalRuleIds.has(ruleId);

        this.logger.error({
          ruleId: rule.ruleId,
          isCritical,
          transactionId: transaction.transactionId,
          error: settled.reason instanceof Error ? settled.reason.message : 'Unknown error',
        }, isCritical 
          ? 'CRITICAL rule evaluation failed — forcing fail-closed'
          : 'Rule evaluation failed, defaulting to safe score');

        if (isCritical) {
          criticalRuleFailed = true;
          failedCriticalRuleId = ruleId;
        }

        // Non-critical failures contribute a zero score (fail-open)
        results.push({
          isSuspicious: false,
          riskScore: 0.0,
          reason: `Rule ${rule.ruleId} failed: ${settled.reason instanceof Error ? settled.reason.message : 'Unknown error'}`,
        });
      }
    });

    // Fail-closed: a critical rule failure forces maximum risk regardless of other scores
    if (criticalRuleFailed) {
      const end = process.hrtime.bigint();
      this.evaluationLatency.labels(environment).observe(Number(end - start) / 1e9);
      return {
        isSuspicious: true,
        aggregateScore: 1.0,
        results,
      };
    }

    // Aggregate scoring: arithmetic mean of risk scores
    const totalScore = results.reduce((acc, curr) => acc + curr.riskScore, 0);
    const aggregateScore = results.length > 0 ? totalScore / results.length : 0;
    const isSuspicious = aggregateScore >= this.threshold;

    const end = process.hrtime.bigint();
    this.evaluationLatency.labels(environment).observe(Number(end - start) / 1e9);

    return {
      isSuspicious,
      aggregateScore,
      results,
    };
  }
}

/**
 * Interface for rules that support hot-reloading their configuration.
 */
export interface HotReloadableRule {
  reloadConfig(config: SystemConfiguration): void;
}

/** Type guard for rules that support hot-reloading. */
function isHotReloadable(rule: FraudRule): rule is FraudRule & HotReloadableRule {
  return typeof (rule as any).reloadConfig === 'function';
}
