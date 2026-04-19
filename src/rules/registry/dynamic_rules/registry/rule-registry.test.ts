import { Registry } from 'prom-client';
import { RuleRegistry } from './rule-registry';
import { FraudRule, RuleEvaluationResult } from '../contracts/fraud-rule-contract';
import { Transaction } from '../../../../core/domain_models/definitions/transaction.interface';

describe('RuleRegistry', () => {
  let ruleRegistry: RuleRegistry;
  let registry: Registry;

  beforeEach(() => {
    registry = new Registry();
    process.env.FRAUD_THRESHOLD = '0.5';
    ruleRegistry = RuleRegistry.initialize(registry);
    (ruleRegistry as any).rules = new Map(); // Clear rules
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('should register a rule', () => {
    const rule: FraudRule = {
      ruleId: 'rule-1',
      description: 'test-rule',
      evaluate: jest.fn(),
    };

    ruleRegistry.registerRule(rule);
    expect((ruleRegistry as any).rules.has('rule-1')).toBe(true);
  });

  it('should aggregate scores with arithmetic mean and trigger suspicion if threshold reached', async () => {
    const rule1: FraudRule = {
      ruleId: 'rule-1',
      description: 'rule 1',
      evaluate: jest.fn().mockResolvedValue({ isSuspicious: true, riskScore: 0.8, reason: 'r1' }),
    };

    const rule2: FraudRule = {
      ruleId: 'rule-2',
      description: 'rule 2',
      evaluate: jest.fn().mockResolvedValue({ isSuspicious: false, riskScore: 0.0, reason: 'r2' }),
    };

    ruleRegistry.registerRule(rule1);
    ruleRegistry.registerRule(rule2);

    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-1',
      userId: 'u-1',
      merchantId: 'm-1',
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: BigInt(Date.now()),
      telemetry: {} as any,
    } as any;

    const evaluation = await ruleRegistry.evaluateAll(transaction);

    // Score: (0.8 + 0.0) / 2 = 0.4. Threshold: 0.5
    expect(evaluation.aggregateScore).toBe(0.4);
    expect(evaluation.isSuspicious).toBe(false);
  });

  it('should trigger suspicion if aggregate score >= threshold', async () => {
    const rule1: FraudRule = {
      ruleId: 'rule-1',
      description: 'rule 1',
      evaluate: jest.fn().mockResolvedValue({ isSuspicious: true, riskScore: 0.8, reason: 'r1' }),
    };

    const rule2: FraudRule = {
      ruleId: 'rule-2',
      description: 'rule 2',
      evaluate: jest.fn().mockResolvedValue({ isSuspicious: true, riskScore: 0.6, reason: 'r2' }),
    };

    ruleRegistry.registerRule(rule1);
    ruleRegistry.registerRule(rule2);

    const transaction: Transaction = {
      type: 'TransactionInitiated',
      transactionId: 'tx-1',
      userId: 'u-1',
      merchantId: 'm-1',
      amount: { value: 100n, currency: 'USD' } as any,
      timestamp: BigInt(Date.now()),
      telemetry: {} as any,
    } as any;

    const evaluation = await ruleRegistry.evaluateAll(transaction);

    // Score: (0.8 + 0.6) / 2 = 0.7. Threshold: 0.5
    expect(evaluation.aggregateScore).toBe(0.7);
    expect(evaluation.isSuspicious).toBe(true);
  });
});
