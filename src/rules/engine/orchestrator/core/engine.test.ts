import { EngineCoreOrchestrator } from './engine';

describe('EngineCoreOrchestrator', () => {
  let mockRuleRegistry: any;
  let mockAlertSubsystem: any;
  let mockMetrics: any;
  let mockRiskAggregator: any;
  let engine: EngineCoreOrchestrator;

  beforeEach(() => {
    mockRuleRegistry = {
      rules: new Map()
    };
    mockAlertSubsystem = {
      dispatchFlag: jest.fn()
    };
    mockMetrics = {
      incrementErrorCount: jest.fn(),
      observeEndToEndLatency: jest.fn()
    };
    mockRiskAggregator = {
      aggregate: jest.fn()
    };

    engine = new EngineCoreOrchestrator(
      mockRuleRegistry,
      mockRiskAggregator,
      mockAlertSubsystem,
      mockMetrics,
      { fraudThreshold: 0.8, environment: 'test' }
    );
  });

  it('should reject unvalidated transactions', async () => {
    const tx = { type: 'TransactionInitiated' } as any;
    await engine.orchestrate(tx);
    expect(mockMetrics.incrementErrorCount).toHaveBeenCalledWith('test', 'INTERNAL_SYSTEM_ERROR');
  });

  it('should run rules and dispatch alert if threshold exceeded', async () => {
    const tx = { type: 'TransactionValidated', transactionId: 'tx1' } as any;

    // Add a dummy rule
    mockRuleRegistry.rules.set('rule1', {
      ruleId: 'rule1',
      evaluate: jest.fn().mockResolvedValue({ riskScore: 0.9, isSuspicious: true, reason: 'bad' })
    });

    mockRiskAggregator.aggregate.mockReturnValue(0.9);

    await engine.orchestrate(tx);

    expect(mockRiskAggregator.aggregate).toHaveBeenCalled();
    expect(mockAlertSubsystem.dispatchFlag).toHaveBeenCalled();
    expect(mockMetrics.observeEndToEndLatency).toHaveBeenCalled();
  });
});
