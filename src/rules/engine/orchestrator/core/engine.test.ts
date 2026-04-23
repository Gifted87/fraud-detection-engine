import { EngineCoreOrchestrator } from './engine';
import { asRiskScore } from '../contracts/engine-contracts';

describe('EngineCoreOrchestrator', () => {
  let mockRuleRegistry: any;
  let mockAlertSubsystem: any;
  let mockMetrics: any;
  let mockRiskAggregator: any;
  let mockLogger: any;
  let engine: EngineCoreOrchestrator;

  beforeEach(() => {
    mockRuleRegistry = {
      rules: new Map(),
      getRules: jest.fn().mockImplementation(() => Array.from(mockRuleRegistry.rules.values())),
      reloadAll: jest.fn()
    };
    mockAlertSubsystem = {
      dispatchFlag: jest.fn().mockResolvedValue(undefined)
    };
    mockMetrics = {
      incrementErrorCount: jest.fn(),
      observeEndToEndLatency: jest.fn()
    };
    mockRiskAggregator = {
      aggregate: jest.fn()
    };
    mockLogger = {
      info: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
      fatal: jest.fn(),
      debug: jest.fn()
    };

    engine = new EngineCoreOrchestrator({
      ruleRegistry: mockRuleRegistry,
      riskAggregator: mockRiskAggregator,
      alertingSubsystem: mockAlertSubsystem,
      orchestrationMetrics: mockMetrics,
      config: { 
        FRAUD_THRESHOLD: 0.8, 
        NODE_ENV: 'test',
        CRITICAL_RULE_IDS: ['merchant-blacklist-rule-v1', 'geospatial-rule-v1']
      } as any,
      logger: mockLogger
    });
  });

  it('should reject unvalidated transactions', async () => {
    const tx = { transactionId: 'tx1', type: 'TransactionInitiated' } as any;
    await expect(engine.orchestrate(tx)).rejects.toThrow('Only validated transactions can be orchestrated');
    expect(mockMetrics.incrementErrorCount).toHaveBeenCalledWith('test', 'INTERNAL_SYSTEM_ERROR');
  });

  it('should run rules and dispatch alert if threshold exceeded', async () => {
    const tx = { 
      type: 'TransactionValidated', 
      transactionId: 'tx1',
      userId: 'u1',
      merchantId: 'm1',
      amount: { value: 100n, currency: 'USD' },
      telemetry: { latitude: 0, longitude: 0 }
    } as any;

    mockRuleRegistry.rules.set('rule1', {
      ruleId: 'rule1',
      evaluate: jest.fn().mockResolvedValue({ riskScore: 0.9, isSuspicious: true, reason: 'bad' })
    });

    mockRiskAggregator.aggregate.mockReturnValue(asRiskScore(0.9));

    await engine.orchestrate(tx);

    expect(mockRuleRegistry.getRules).toHaveBeenCalled();
    expect(mockRiskAggregator.aggregate).toHaveBeenCalled();
    expect(mockAlertSubsystem.dispatchFlag).toHaveBeenCalled();
    expect(mockMetrics.observeEndToEndLatency).toHaveBeenCalled();
  });

  it('should trigger fail-closed for critical rule failures', async () => {
    const tx = { 
      type: 'TransactionValidated', 
      transactionId: 'tx1',
      userId: 'critical-user'
    } as any;

    const criticalEngine = new EngineCoreOrchestrator({
      ruleRegistry: mockRuleRegistry,
      riskAggregator: mockRiskAggregator,
      alertingSubsystem: mockAlertSubsystem,
      orchestrationMetrics: mockMetrics,
      config: { 
        FRAUD_THRESHOLD: 0.8, 
        NODE_ENV: 'test',
        CRITICAL_RULE_IDS: ['merchant-blacklist-rule-v1']
      } as any,
      logger: mockLogger
    });

    mockRuleRegistry.rules.set('merchant-blacklist-rule-v1', {
      ruleId: 'merchant-blacklist-rule-v1',
      evaluate: jest.fn().mockRejectedValue(new Error('Infrastructure Down'))
    });

    await criticalEngine.orchestrate(tx);

    expect(mockAlertSubsystem.dispatchFlag).toHaveBeenCalled();
    const callArgs = mockAlertSubsystem.dispatchFlag.mock.calls[0];
    expect(callArgs[6]).toBe(1.0); // riskScore argument
  });
});
