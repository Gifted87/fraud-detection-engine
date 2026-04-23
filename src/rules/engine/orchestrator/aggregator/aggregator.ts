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
import { Logger } from 'pino';
import { SystemConfiguration } from '../../../../core/domain_models/dependency_config';

interface Dependencies {
  metricsCollector: MetricsCollector;
  config: SystemConfiguration;
  logger: Logger;
}

/**
 * Concrete implementation of the RiskAggregator.
 * Uses a weighted mean algorithm to produce a deterministic risk score.
 */
export class WeightedRiskAggregator implements RiskAggregator {
  private readonly weights: Record<string, number>;
  private readonly metrics: MetricsCollector;
  private readonly logger: Logger;
  private readonly environment: 'production' | 'development' | 'test';
  private readonly defaultWeight: number = 1.0;

  constructor({ metricsCollector, config, logger }: Dependencies) {
    this.weights = config.RULE_WEIGHTS;
    this.metrics = metricsCollector;
    this.logger = logger;
    this.environment = config.NODE_ENV;
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
      environment: this.environment,
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
      this.logger.error({
        results_snapshot: results,
        error: error instanceof Error ? error.message : 'Unknown error'
      }, 'Risk aggregation failed, defaulting to safe score');

      // Report failure to metrics
      this.metrics.incrementThroughput(metricLabels, 'aggregation_failure');
      
      return asRiskScore(0.0);
    } finally {
      // Measure aggregation latency
      this.metrics.observeLatency(metricLabels, 'aggregate', startNs);
    }
  }
}
