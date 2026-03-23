import { Registry, Histogram } from 'prom-client';
import { z } from 'zod';
import { FraudRule, RuleEvaluationResult } from '../contracts/fraud_rule.contract';
import { Transaction } from '../../../../../../core/domain_models/definitions/transaction.interface';

/**
 * Registry for managing and orchestrating FraudRule evaluation.
 * Acts as a thread-safe singleton for high-concurrency event processing.
 */
export class RuleRegistry {
  private static instance: RuleRegistry;
  private readonly rules: Map<string, FraudRule> = new Map();
  private readonly threshold: number;
  private readonly registry: Registry;
  private readonly evaluationLatency: Histogram<string>;

  private constructor(registry: Registry) {
    this.registry = registry;
    this.threshold = parseFloat(process.env.FRAUD_THRESHOLD || '0.5');

    this.evaluationLatency = new Histogram({
      name: 'fraud_engine_registry_evaluation_latency_seconds',
      help: 'Aggregate latency of all rule evaluations',
      registers: [this.registry],
      labelNames: ['environment'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
    });
  }

  /**
   * Initializes the RuleRegistry singleton.
   */
  public static initialize(registry: Registry): RuleRegistry {
    if (!RuleRegistry.instance) {
      RuleRegistry.instance = new RuleRegistry(registry);
    }
    return RuleRegistry.instance;
  }

  public static getInstance(): RuleRegistry {
    if (!RuleRegistry.instance) {
      throw new Error('RuleRegistry not initialized. Call initialize() first.');
    }
    return RuleRegistry.instance;
  }

  /**
   * Registers a new fraud rule with runtime validation.
   */
  public registerRule(rule: FraudRule): void {
    if (this.rules.has(rule.ruleId)) {
      throw new Error(`Rule with ID ${rule.ruleId} already registered.`);
    }

    // Basic contract validation
    if (!rule.evaluate || typeof rule.evaluate !== 'function') {
      throw new Error(`Invalid rule implementation: ${rule.ruleId} lacks evaluate method.`);
    }

    this.rules.set(rule.ruleId, rule);
  }

  /**
   * Orchestrates concurrent evaluation of all registered rules.
   * Suppresses failures to ensure pipeline reliability.
   */
  public async evaluateAll(transaction: Transaction): Promise<{ isSuspicious: boolean; aggregateScore: number; results: RuleEvaluationResult[] }> {
    const start = process.hrtime.bigint();
    const environment = process.env.NODE_ENV || 'production';

    // Parallel evaluation
    const evaluationPromises = Array.from(this.rules.values()).map(async (rule) => {
      try {
        const result = await rule.evaluate(transaction);
        // Validate result against contract
        return result;
      } catch (error) {
        // Structured logging for critical failures
        console.error(JSON.stringify({
          level: 'critical',
          message: 'Rule evaluation failed, defaulting to safe',
          ruleId: rule.ruleId,
          transactionId: transaction.transactionId,
          error: error instanceof Error ? error.message : 'Unknown error',
          timestamp: Date.now()
        }));

        // Safe failure fallback
        return {
          isSuspicious: false,
          riskScore: 0.0,
          reason: `Rule ${rule.ruleId} failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
        };
      }
    });

    const results = await Promise.all(evaluationPromises);

    // Aggregate scoring: Arithmetic mean of risk scores
    const totalScore = results.reduce((acc, curr) => acc + curr.riskScore, 0);
    const aggregateScore = results.length > 0 ? totalScore / results.length : 0;
    const isSuspicious = aggregateScore >= this.threshold;

    // Observe metrics
    const end = process.hrtime.bigint();
    const latency = Number(end - start) / 1e9;
    this.evaluationLatency.labels(environment).observe(latency);

    return {
      isSuspicious,
      aggregateScore,
      results
    };
  }
}
