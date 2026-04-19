import { Histogram, Registry } from 'prom-client';
import { FraudRule, RuleEvaluationResult } from '../../contracts/fraud-rule-contract';
import { Transaction } from '../../../../../core/domain_models/definitions/transaction.interface';

/**
 * MerchantBlacklistRule
 * 
 * Flags transactions associated with merchants on a prohibited list.
 * This list can be dynamically configured via environment variables.
 */
export class MerchantBlacklistRule implements FraudRule {
  public readonly ruleId: string = 'merchant-blacklist-rule-v1';
  public readonly description: string = 'Flags transactions from merchants on a prohibited blacklist.';

  private readonly blacklist: Set<string>;
  private readonly metrics: Histogram<string>;

  constructor(
    private readonly registry: Registry
  ) {
    // Load blacklisted merchant IDs from environment (comma-separated string)
    const blacklistRaw = process.env.MERCHANT_BLACKLIST || '';
    this.blacklist = new Set(blacklistRaw.split(',').map(id => id.trim()).filter(id => id.length > 0));

    this.metrics = new Histogram({
      name: 'fraud_engine_rule_merchant_blacklist_latency_seconds',
      help: 'Latency of merchant blacklist rule evaluation',
      registers: [this.registry],
      labelNames: ['ruleId', 'environment'],
      buckets: [0.001, 0.002, 0.005, 0.01, 0.025, 0.05],
    });
  }

  /**
   * Evaluates if a transaction involves a blacklisted merchant.
   */
  public async evaluate(transaction: Transaction): Promise<RuleEvaluationResult> {
    const start = process.hrtime.bigint();
    const environment = process.env.NODE_ENV || 'production';

    try {
      // 1. Check if the merchant ID is in the blacklist
      const isBlacklisted = this.blacklist.has(transaction.merchantId);

      // 2. Evaluate against threshold
      const result: RuleEvaluationResult = Object.freeze({
        isSuspicious: isBlacklisted,
        riskScore: isBlacklisted ? 1.0 : 0.0,
        reason: isBlacklisted 
          ? `Merchant ${transaction.merchantId} is on the prohibited blacklist.`
          : 'Merchant is not blacklisted',
        metadata: {
          merchantId: transaction.merchantId,
          blacklistSize: this.blacklist.size
        }
      });

      // 3. Structured Logging
      if (isBlacklisted) {
        console.log(JSON.stringify({
          ruleId: this.ruleId,
          transactionId: transaction.transactionId,
          merchantId: transaction.merchantId,
          isSuspicious: result.isSuspicious,
          timestamp: Date.now()
        }));
      }

      return result;

    } catch (error) {
      // 4. Defensive fallback on error to prevent pipeline blockage
      console.error(JSON.stringify({
        level: 'critical',
        message: 'MerchantBlacklistRule evaluation failed',
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
