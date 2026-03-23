import { Registry, Histogram, Counter } from 'prom-client';

/**
 * EventStoreMetrics handles high-precision, low-overhead observability for the 
 * PostgreSQL-backed Event Store.
 * 
 * Provides P99/P95 latency tracking and operation throughput metrics.
 */
export class EventStoreMetrics {
  private static instance: EventStoreMetrics;
  private readonly registry: Registry;

  public readonly appendDuration: Histogram<string>;
  public readonly loadDuration: Histogram<string>;
  public readonly operationsTotal: Counter<string>;
  public readonly errorsTotal: Counter<string>;

  // Optimized buckets for sub-millisecond to multi-second operations
  private readonly buckets = [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0];

  private constructor(registry: Registry) {
    this.registry = registry;

    this.appendDuration = new Histogram({
      name: 'event_store_append_duration_seconds',
      help: 'Duration of the append operation in seconds',
      registers: [this.registry],
      buckets: this.buckets,
    });

    this.loadDuration = new Histogram({
      name: 'event_store_load_duration_seconds',
      help: 'Duration of the load operation in seconds',
      registers: [this.registry],
      buckets: this.buckets,
    });

    this.operationsTotal = new Counter({
      name: 'event_store_operations_total',
      help: 'Total number of operations performed',
      registers: [this.registry],
      labelNames: ['operation_type', 'outcome'],
    });

    this.errorsTotal = new Counter({
      name: 'event_store_errors_total',
      help: 'Total number of domain-specific errors',
      registers: [this.registry],
      labelNames: ['error_type'],
    });
  }

  /**
   * Initializes the singleton instance of EventStoreMetrics.
   */
  public static initialize(registry: Registry): EventStoreMetrics {
    if (!EventStoreMetrics.instance) {
      EventStoreMetrics.instance = new EventStoreMetrics(registry);
    }
    return EventStoreMetrics.instance;
  }

  /**
   * Returns the singleton instance.
   */
  public static getInstance(): EventStoreMetrics {
    if (!EventStoreMetrics.instance) {
      throw new Error('EventStoreMetrics has not been initialized');
    }
    return EventStoreMetrics.instance;
  }

  /**
   * Records the duration of an operation.
   * @param histogram The metric to observe.
   * @param startHrTime The start time obtained via process.hrtime.bigint().
   */
  public recordDuration(histogram: Histogram<string>, startHrTime: bigint): void {
    const endHrTime = process.hrtime.bigint();
    const durationSeconds = Number(endHrTime - startHrTime) / 1e9;
    histogram.observe(durationSeconds);
  }

  /**
   * Increments the operations counter.
   */
  public incrementOperation(operationType: 'append' | 'load', outcome: 'success' | 'failure'): void {
    this.operationsTotal.labels(operationType, outcome).inc();
  }

  /**
   * Increments the errors counter.
   */
  public incrementError(errorType: string): void {
    this.errorsTotal.labels(errorType).inc();
  }
}
