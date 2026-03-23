/**
 * @fileoverview MetricsProvider service for the fraud detection consumer pipeline.
 * Provides high-resolution, thread-safe, and non-blocking observability for
 * Kafka event processing. Adheres to the Prometheus monitoring standard.
 */

import { Registry, Histogram, Counter, Gauge } from 'prom-client';

/**
 * Strict label schema for Prometheus metrics.
 * Ensures consistent data structure across the observation stack.
 */
export interface ConsumerMetricLabels {
  readonly environment: 'development' | 'production' | 'test';
  readonly stream: string;
  readonly error_type?: 'validation' | 'crypto' | 'persistence' | 'unknown';
}

/**
 * MetricsProvider encapsulates the Prometheus registry and metric definitions
 * for the fraud detection Kafka consumer.
 */
export class MetricsProvider {
  private static instance: MetricsProvider;
  private readonly registry: Registry;

  // Histogram: Processing latency (poll to commit)
  private readonly processingLatency: Histogram<string>;
  // Counter: Throughput
  private readonly eventsProcessedTotal: Counter<string>;
  // Counter: Errors
  private readonly errorTotal: Counter<string>;
  // Counter: DLQ Ingress
  private readonly dlqIngressTotal: Counter<string>;
  // Gauge: Consumer State
  private readonly consumerState: Gauge<string>;
  // Gauge: Rebalance Count
  private readonly rebalanceCount: Gauge<string>;

  /**
   * Private constructor to enforce Singleton pattern.
   * Defines metric configurations with tuned exponential buckets for financial latency monitoring.
   */
  private constructor(registry: Registry) {
    this.registry = registry;

    this.processingLatency = new Histogram({
      name: 'fraud_consumer_processing_latency_seconds',
      help: 'Latency of the consumption cycle from Kafka poll to commit',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
      buckets: [0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1.0, 2.0],
    });

    this.eventsProcessedTotal = new Counter({
      name: 'fraud_consumer_events_processed_total',
      help: 'Total successfully processed events',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
    });

    this.errorTotal = new Counter({
      name: 'fraud_consumer_errors_total',
      help: 'Total processing errors encountered',
      registers: [this.registry],
      labelNames: ['environment', 'stream', 'error_type'],
    });

    this.dlqIngressTotal = new Counter({
      name: 'fraud_consumer_dlq_ingress_total',
      help: 'Total events routed to Dead Letter Queue',
      registers: [this.registry],
      labelNames: ['environment', 'stream', 'error_type'],
    });

    this.consumerState = new Gauge({
      name: 'fraud_consumer_active_state',
      help: 'Kafka consumer state (1 for active, 0 for paused/rebalancing)',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
    });

    this.rebalanceCount = new Gauge({
      name: 'fraud_consumer_rebalance_count_total',
      help: 'Total number of consumer group rebalances observed',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
    });
  }

  /**
   * Initializes or returns the MetricsProvider singleton.
   * @param registry The Prometheus Registry instance to attach metrics to.
   */
  public static initialize(registry: Registry): MetricsProvider {
    if (!MetricsProvider.instance) {
      MetricsProvider.instance = new MetricsProvider(registry);
    }
    return MetricsProvider.instance;
  }

  /**
   * Returns the initialized MetricsProvider instance.
   * @throws Error if not initialized.
   */
  public static getInstance(): MetricsProvider {
    if (!MetricsProvider.instance) {
      throw new Error('MetricsProvider not initialized. Call initialize() first.');
    }
    return MetricsProvider.instance;
  }

  /**
   * Starts a latency timer for a consumption cycle.
   * @returns A high-precision nanosecond timestamp (bigint).
   */
  public startTimer(): bigint {
    return process.hrtime.bigint();
  }

  /**
   * Records the end-to-end processing latency for a completed cycle.
   * @param labels Metric labels.
   * @param startNs Start timestamp from startTimer().
   */
  public observeProcessingLatency(labels: ConsumerMetricLabels, startNs: bigint): void {
    try {
      const endNs = process.hrtime.bigint();
      const durationSeconds = Number(endNs - startNs) / 1e9;
      this.processingLatency.labels(labels.environment, labels.stream).observe(durationSeconds);
    } catch (e) {
      // Fail-safe: Silently ignore recording failure to protect primary logic
    }
  }

  /**
   * Increments the successfully processed event counter.
   */
  public incrementProcessedEvents(labels: ConsumerMetricLabels): void {
    try {
      this.eventsProcessedTotal.labels(labels.environment, labels.stream).inc();
    } catch (e) {
      // Fail-safe
    }
  }

  /**
   * Increments error counts for specific failure categories.
   */
  public incrementErrorCount(labels: ConsumerMetricLabels): void {
    try {
      this.errorTotal.labels(labels.environment, labels.stream, labels.error_type || 'unknown').inc();
    } catch (e) {
      // Fail-safe
    }
  }

  /**
   * Increments the DLQ ingress counter when processing fails beyond retries.
   */
  public incrementDlqIngress(labels: ConsumerMetricLabels): void {
    try {
      this.dlqIngressTotal.labels(labels.environment, labels.stream, labels.error_type || 'unknown').inc();
    } catch (e) {
      // Fail-safe
    }
  }

  /**
   * Sets the consumer active state gauge.
   * @param isActive True if consuming, false otherwise.
   */
  public setConsumerState(labels: ConsumerMetricLabels, isActive: boolean): void {
    try {
      this.consumerState.labels(labels.environment, labels.stream).set(isActive ? 1 : 0);
    } catch (e) {
      // Fail-safe
    }
  }

  /**
   * Increments the consumer group rebalance counter.
   */
  public incrementRebalanceCount(labels: ConsumerMetricLabels): void {
    try {
      this.rebalanceCount.labels(labels.environment, labels.stream).inc();
    } catch (e) {
      // Fail-safe
    }
  }

  /**
   * Returns the current registry snapshot (if required for health checks).
   */
  public getRegistry(): Registry {
    return this.registry;
  }
}
