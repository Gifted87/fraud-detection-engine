import { WeightedRiskAggregator } from './aggregator';
import { MetricsCollector } from '../../../../utils/metrics/metrics-collector';

describe('WeightedRiskAggregator', () => {
  let mockMetrics: any;
  let aggregator: WeightedRiskAggregator;

  beforeEach(() => {
    mockMetrics = {
      incrementThroughput: jest.fn(),
      observeLatency: jest.fn()
    };
    aggregator = new WeightedRiskAggregator({ rule1: 2.0, rule2: 1.0 }, mockMetrics as any);
  });

  it('should compute weighted aggregate correctly', () => {
    const results = [
      { ruleId: 'rule1', score: 0.8, isSuspicious: true, findings: '', timestamp: 1n },
      { ruleId: 'rule2', score: 0.2, isSuspicious: false, findings: '', timestamp: 1n }
    ];

    // (0.8 * 2.0 + 0.2 * 1.0) / 3.0 = (1.6 + 0.2) / 3.0 = 1.8 / 3.0 = 0.6
    const risk = aggregator.aggregate(results as any);
    expect(risk).toBeCloseTo(0.6);
    expect(mockMetrics.observeLatency).toHaveBeenCalled();
  });

  it('should return 0.0 for empty results', () => {
    const risk = aggregator.aggregate([]);
    expect(risk).toBe(0.0);
  });

  it('should apply default weight for unknown rules', () => {
    const results = [
      { ruleId: 'unknown', score: 0.5, isSuspicious: true, findings: '', timestamp: 1n }
    ];
    // weight is 1.0, score 0.5 -> 0.5/1 = 0.5
    const risk = aggregator.aggregate(results as any);
    expect(risk).toBe(0.5);
  });
});
