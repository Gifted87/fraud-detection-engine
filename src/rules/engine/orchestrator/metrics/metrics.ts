/**
 * @fileoverview Performance Instrumentation Layer for the Orchestrator.
 * Provides high-precision latency tracking and Prometheus metrics collection
 * for the fraud detection pipeline.
 */

import { Registry, Histogram, Counter } from 'prom-client';

/**
 * Defines the strict label schema for metrics to prevent cardinality explosion.
 */
export interface OrchestratorMetricLabels {
  readonly environment: 'development' | 'production' | 'test';
  readonly ruleId?: string;
}

/**
 * Singleton-pattern performance instrumentation collector.
 * Manages Prometheus registries and provides lock-free, fail-safe metric recording.
 */
export class OrchestrationMetricsCollector {
  private static instance: OrchestrationMetricsCollector;
  private readonly registry: Registry;

  // Metric definitions
  private readonly endToEndLatency: Histogram<string>;
  private readonly ruleEvaluationLatency: Histogram<string>;
  private readonly errorCounter: Counter<string>;
  private readonly flaggingCounter: Counter<string>;

  // Histogram buckets optimized for P95/P99 latency in Node.js financial backends.
  // Covers range from 1ms to 2 seconds, with fine-grained buckets 10ms-100ms.
  private readonly latencyBuckets = [
    0.001, 0.002, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.25, 0.5, 1.0, 2.0
  ];

  private constructor(registry: Registry) {
    this.registry = registry;

    this.endToEndLatency = new Histogram({
      name: 'fraud_orchestrator_e2e_latency_seconds',
      help: 'Total orchestration cycle latency (validated event to decision)',
      registers: [this.registry],
      labelNames: ['environment'],
      buckets: this.latencyBuckets,
    });

    this.ruleEvaluationLatency = new Histogram({
      name: 'fraud_orchestrator_rule_latency_seconds',
      help: 'Per-rule execution latency',
      registers: [this.registry],
      labelNames: ['environment', 'ruleId'],
      buckets: this.latencyBuckets,
    });

    this.errorCounter = new Counter({
      name: 'fraud_orchestrator_errors_total',
      help: 'Orchestration or rule-level errors',
      registers: [this.registry],
      labelNames: ['environment', 'ruleId', 'errorType'],
    });

    this.flaggingCounter = new Counter({
      name: 'fraud_orchestrator_flagging_throughput_total',
      help: 'Flagging throughput: total flagged vs total processed',
      registers: [this.registry],
      labelNames: ['environment', 'outcome'], // 'outcome' = 'flagged' | 'approved'
    });
  }

  /**
   * Initializes the MetricsCollector singleton.
   */
  public static initialize(registry: Registry): OrchestrationMetricsCollector {
    if (!OrchestrationMetricsCollector.instance) {
      OrchestrationMetricsCollector.instance = new OrchestrationMetricsCollector(registry);
    }
    return OrchestrationMetricsCollector.instance;
  }

  /**
   * Returns the singleton instance of the MetricsCollector.
   */
  public static getInstance(): OrchestrationMetricsCollector {
    if (!OrchestrationMetricsCollector.instance) {
      throw new Error('OrchestrationMetricsCollector not initialized. Call initialize() first.');
    }
    return OrchestrationMetricsCollector.instance;
  }

  /**
   * Observes E2E latency for an orchestration cycle.
   * @param env Environment identifier.
   * @param startNs Start time in nanoseconds.
   */
  public observeEndToEndLatency(env: 'development' | 'production' | 'test', startNs: bigint): void {
    try {
      const endNs = process.hrtime.bigint();
      const durationSeconds = Number(endNs - startNs) / 1e9;
      this.endToEndLatency.labels(env).observe(durationSeconds);
    } catch (e) {
      this.logMetricsError('observeEndToEndLatency', e);
    }
  }

  /**
   * Observes latency for an individual rule execution.
   * @param env Environment identifier.
   * @param ruleId Rule identifier.
   * @param startNs Start time in nanoseconds.
   */
  public observeRuleLatency(
    env: 'development' | 'production' | 'test',
    ruleId: string,
    startNs: bigint
  ): void {
    try {
      const endNs = process.hrtime.bigint();
      const durationSeconds = Number(endNs - startNs) / 1e9;
      this.ruleEvaluationLatency.labels(env, ruleId).observe(durationSeconds);
    } catch (e) {
      this.logMetricsError('observeRuleLatency', e);
    }
  }

  /**
   * Increments the error counter for the orchestration pipeline.
   * @param env Environment identifier.
   * @param ruleId Rule identifier if applicable.
   * @param errorType Type of error (e.g., 'timeout', 'execution_failure').
   */
  public incrementErrorCount(
    env: 'development' | 'production' | 'test',
    errorType: string,
    ruleId: string = 'system'
  ): void {
    try {
      this.errorCounter.labels(env, ruleId, errorType).inc();
    } catch (e) {
      this.logMetricsError('incrementErrorCount', e);
    }
  }

  /**
   * Increments the flagging throughput counter.
   * @param env Environment identifier.
   * @param isFlagged Boolean indicator of flagging status.
   */
  public recordThroughput(
    env: 'development' | 'production' | 'test',
    isFlagged: boolean
  ): void {
    try {
      const outcome = isFlagged ? 'flagged' : 'approved';
      this.flaggingCounter.labels(env, outcome).inc();
    } catch (e) {
      this.logMetricsError('recordThroughput', e);
    }
  }

  /**
   * Fail-safe logger for metrics errors.
   */
  private logMetricsError(operation: string, error: unknown): void {
    console.error(JSON.stringify({
      level: 'error',
      component: 'metrics_instrumentation',
      operation,
      message: error instanceof Error ? error.message : 'Unknown instrumentation error',
      timestamp: Date.now(),
    }));
  }
}
