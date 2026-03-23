import { Registry, Histogram, Counter, Gauge } from 'prom-client';

/**
 * Interface representing the allowed metric label schema.
 * Prevents cardinality explosion by strictly defining key-value pairs.
 */
export interface MetricLabels {
  readonly environment: 'development' | 'production' | 'test';
  readonly component: string;
  readonly stream_name: string;
}

/**
 * MetricsCollector provides a centralized, highly-optimized observability interface
 * for tracking system performance and health in the fraud detection engine.
 * 
 * Implements a push-based model using Prometheus client, ensuring sub-microsecond
 * latency measurements via process.hrtime.bigint().
 */
export class MetricsCollector {
  private static instance: MetricsCollector;
  private readonly registry: Registry;
  private readonly latencyBuckets = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0];

  private readonly latencies: Histogram<string>;
  private readonly throughput: Counter<string>;
  private readonly states: Gauge<string>;

  private constructor(registry: Registry) {
    this.registry = registry;

    this.latencies = new Histogram({
      name: 'fraud_engine_operation_latency_seconds',
      help: 'Histogram for tracking P95/P99 operation latencies',
      registers: [this.registry],
      labelNames: ['environment', 'component', 'stream_name', 'operation'],
      buckets: this.latencyBuckets,
    });

    this.throughput = new Counter({
      name: 'fraud_engine_throughput_total',
      help: 'Total events ingested or operations executed',
      registers: [this.registry],
      labelNames: ['environment', 'component', 'stream_name', 'type'],
    });

    this.states = new Gauge({
      name: 'fraud_engine_system_state',
      help: 'Instantaneous system state gauges',
      registers: [this.registry],
      labelNames: ['environment', 'component', 'metric_name'],
    });
  }

  /**
   * Initializes the MetricsCollector singleton.
   * @param registry Shared Prometheus registry instance.
   */
  public static initialize(registry: Registry): MetricsCollector {
    if (!MetricsCollector.instance) {
      MetricsCollector.instance = new MetricsCollector(registry);
    }
    return MetricsCollector.instance;
  }

  /**
   * Returns the singleton instance of the MetricsCollector.
   */
  public static getInstance(): MetricsCollector {
    if (!MetricsCollector.instance) {
      throw new Error('MetricsCollector has not been initialized. Call initialize() first.');
    }
    return MetricsCollector.instance;
  }

  /**
   * Records the duration of an operation.
   * Uses process.hrtime.bigint() for sub-microsecond resolution.
   * 
   * @param labels Metric label configuration.
   * @param operation Name of the operation being measured.
   * @param startNs Start time in nanoseconds.
   */
  public observeLatency(labels: MetricLabels, operation: string, startNs: bigint): void {
    try {
      const endNs = process.hrtime.bigint();
      const durationSeconds = Number(endNs - startNs) / 1e9;
      this.latencies.labels(labels.environment, labels.component, labels.stream_name, operation).observe(durationSeconds);
    } catch (err) {
      // Fail-safe: Silently ignore metrics collection failures to not affect business logic
    }
  }

  /**
   * Increments the operation throughput counter.
   * 
   * @param labels Metric label configuration.
   * @param type The type of event (e.g., 'success', 'error', 'cache_miss').
   */
  public incrementThroughput(labels: MetricLabels, type: string): void {
    try {
      this.throughput.labels(labels.environment, labels.component, labels.stream_name, type).inc();
    } catch (err) {
      // Fail-safe
    }
  }

  /**
   * Sets the gauge for an instantaneous system state.
   * 
   * @param labels Metric label configuration.
   * @param metricName Name of the state metric.
   * @param value The value to set the gauge to.
   */
  public setGauge(labels: MetricLabels, metricName: string, value: number): void {
    try {
      this.states.labels(labels.environment, labels.component, metricName).set(value);
    } catch (err) {
      // Fail-safe
    }
  }

  /**
   * High-order wrapper for instrumenting asynchronous operations.
   * 
   * @param labels Metric label configuration.
   * @param operationName Name of the operation.
   * @param fn The asynchronous operation to wrap.
   */
  public async instrument<T>(labels: MetricLabels, operationName: string, fn: () => Promise<T>): Promise<T> {
    const startNs = process.hrtime.bigint();
    try {
      const result = await fn();
      this.observeLatency(labels, operationName, startNs);
      this.incrementThroughput(labels, 'success');
      return result;
    } catch (err) {
      this.observeLatency(labels, operationName, startNs);
      this.incrementThroughput(labels, 'error');
      throw err;
    }
  }
}
