import { Counter, Registry } from 'prom-client';
import { FraudRule, RuleEvaluationResult } from '../../contracts/fraud-rule-contract';
import { Transaction } from '../../../../../core/domain_models/definitions/transaction.interface';
import { Logger } from 'pino';
import { SystemConfiguration } from '../../../../../core/domain_models/dependency_config';

interface Dependencies {
  registry: Registry;
  logger: Logger;
  config: SystemConfiguration;
}

/**
 * MerchantBlacklistRule
 *
 * Implements a "fail-closed" security policy for high-risk or sanctioned merchants.
 * If a merchant ID appears in the prohibited list, the transaction is immediately
 * flagged with maximum risk (1.0).
 *
 * This rule is treated as CRITICAL by the RuleRegistry. If the engine fails to 
 * evaluate this rule (e.g. configuration corruption), the orchestrator will 
 * trigger a fail-closed response, flagging the transaction by default.
 */
export class MerchantBlacklistRule implements FraudRule {
  public readonly ruleId: string = 'merchant-blacklist-rule-v1';
  public readonly description: string = 'Immediately flags transactions at prohibited or sanctioned merchants.';

  private blacklist: Set<string>;
  private readonly metrics: Counter<string>;
  private readonly logger: Logger;
  private readonly registry: Registry;
  private readonly environment: 'development' | 'production' | 'test';

  constructor({ registry, logger, config }: Dependencies) {
    this.registry = registry;
    this.logger = logger;
    this.environment = config.NODE_ENV;
    this.blacklist = new Set();
    
    this.metrics = new Counter({
      name: 'fraud_engine_rule_merchant_blacklist_hits_total',
      help: 'Total number of transactions hitting the merchant blacklist',
      labelNames: ['ruleId', 'merchantId', 'environment'],
      registers: [this.registry],
    });

    this.reloadConfig(config);
  }

  /**
   * Reloads the prohibited merchant list from the centralized configuration.
   */
  public reloadConfig(config: SystemConfiguration): void {
    const blacklistRaw = config.MERCHANT_BLACKLIST;
    const newBlacklist = new Set(
      blacklistRaw
        .split(',')
        .map((id) => id.trim())
        .filter((id) => id.length > 0)
    );

    if (newBlacklist.size !== this.blacklist.size || [...newBlacklist].some(id => !this.blacklist.has(id))) {
      this.logger.info({ 
        ruleId: this.ruleId, 
        merchantCount: newBlacklist.size 
      }, 'MerchantBlacklist reloaded');
      this.blacklist = newBlacklist;
    }
  }

  /**
   * Checks if the transaction merchant is in the blacklist.
   */
  public async evaluate(transaction: Transaction): Promise<RuleEvaluationResult> {
    const isSuspicious = this.blacklist.has(transaction.merchantId);

    if (isSuspicious) {
      this.metrics.labels(this.ruleId, transaction.merchantId, this.environment).inc();
      this.logger.warn({
        ruleId: this.ruleId,
        transactionId: transaction.transactionId,
        merchantId: transaction.merchantId,
        userId: transaction.userId
      }, 'Merchant blacklist hit detected');
    }

    return Object.freeze({
      isSuspicious,
      riskScore: isSuspicious ? 1.0 : 0.0, // Fail-closed: high risk score for blacklisted merchants
      reason: isSuspicious
        ? `Merchant ${transaction.merchantId} is on the prohibited blacklist.`
        : 'Merchant is not blacklisted',
      metadata: {
        blacklistSize: this.blacklist.size
      }
    });
  }
}
