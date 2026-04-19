/**
 * @fileoverview RiskAggregator Implementation.
 * Transforms individual rule evaluation outcomes into a high-fidelity RiskScore.
 */

import {
  RuleResult,
  RiskScore,
  asRiskScore,
  RiskAggregator,
} from '../contracts/engine-contracts';
import { MetricsCollector } from '../../../../utils/metrics/metrics-collector';

/**
 * Concrete implementation of the RiskAggregator.
 * Uses a weighted mean algorithm to produce a deterministic risk score.
 */
export class WeightedRiskAggregator implements RiskAggregator {
  private readonly weights: Record<string, number>;
  private readonly metrics: MetricsCollector;
  private readonly defaultWeight: number = 1.0;

  /**
   * @param weights Map of rule IDs to their significance weights.
   * @param metrics Observability collector instance.
   */
  constructor(weights: Record<string, number>, metrics: MetricsCollector) {
    this.weights = { ...weights };
    this.metrics = metrics;
  }

  /**
   * Computes a weighted aggregated risk score in the range [0.0, 1.0].
   * 
   * Formula: Σ (score_i * weight_i) / Σ (weight_i)
   * 
   * @param results ReadonlyArray of rule evaluation outcomes.
   * @returns A branded RiskScore.
   */
  public aggregate(results: ReadonlyArray<RuleResult>): RiskScore {
    const startNs = process.hrtime.bigint();
    const metricLabels = {
      environment: (process.env.NODE_ENV as 'development' | 'production' | 'test') || 'production',
      component: 'risk_aggregator',
      stream_name: 'risk_assessment',
    };

    try {
      if (!results || results.length === 0) {
        return asRiskScore(0.0);
      }

      let totalWeightedScore = 0.0;
      let totalWeight = 0.0;

      for (const result of results) {
        const weight = this.weights[result.ruleId] ?? this.defaultWeight;
        
        // Normalize individual score just in case, though it should be [0, 1] per spec.
        const score = Math.max(0.0, Math.min(1.0, result.score));
        
        totalWeightedScore += score * weight;
        totalWeight += weight;
      }

      const rawScore = totalWeight > 0 ? totalWeightedScore / totalWeight : 0.0;

      // Final normalization and branding
      const finalScore = asRiskScore(Math.max(0.0, Math.min(1.0, rawScore)));
      
      return finalScore;
    } catch (error) {
      // Structured logging for diagnostic traceability
      console.error(JSON.stringify({
        level: 'critical',
        message: 'Risk aggregation failed, defaulting to safe score',
        results_snapshot: results,
        error: error instanceof Error ? error.message : 'Unknown error',
        timestamp: Date.now(),
      }));

      // Report failure to metrics
      this.metrics.incrementThroughput(metricLabels, 'aggregation_failure');
      
      return asRiskScore(0.0);
    } finally {
      // Measure aggregation latency
      this.metrics.observeLatency(metricLabels, 'aggregate', startNs);
    }
  }
}
