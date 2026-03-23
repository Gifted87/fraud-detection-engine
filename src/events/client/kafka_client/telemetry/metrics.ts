import { Registry, Histogram, Counter, Gauge } from 'prom-client';
import { z } from 'zod';

/**
 * Zod schema for metric labels to ensure consistency and prevent cardinality explosion.
 */
export const MetricLabelSchema = z.object({
  environment: z.string(),
  stream: z.string(),
  partition: z.string().optional(),
});

export type MetricLabels = z.infer<typeof MetricLabelSchema>;

/**
 * MetricsManager provides a high-precision observability layer for Kafka messaging operations.
 * It follows a singleton pattern to maintain registry consistency across the messaging client lifecycle.
 */
export class MetricsManager {
  private static instance: MetricsManager;
  private readonly registry: Registry;

  // Producer Metrics
  private readonly producerProduceDuration: Histogram<string>;
  private readonly producerEventsAttempted: Counter<string>;
  private readonly producerEventsSuccess: Counter<string>;
  private readonly producerEventsFailed: Counter<string>;

  // Consumer Metrics
  private readonly consumerProcessingDuration: Histogram<string>;
  private readonly consumerEventsConsumed: Counter<string>;
  private readonly consumerRebalanceCount: Counter<string>;
  private readonly consumerDlqIngressCount: Counter<string>;

  private constructor(registry: Registry) {
    this.registry = registry;

    // Define Producer Latency Histogram with P95/P99 tuned buckets (ms)
    this.producerProduceDuration = new Histogram({
      name: 'kafka_producer_produce_duration_seconds',
      help: 'Duration of message produce operations in seconds',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
      buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
    });

    this.producerEventsAttempted = new Counter({
      name: 'kafka_producer_events_attempted_total',
      help: 'Total events attempted to produce',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
    });

    this.producerEventsSuccess = new Counter({
      name: 'kafka_producer_events_success_total',
      help: 'Total successful produce operations',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
    });

    this.producerEventsFailed = new Counter({
      name: 'kafka_producer_events_failed_total',
      help: 'Total failed produce operations',
      registers: [this.registry],
      labelNames: ['environment', 'stream', 'reason'],
    });

    // Define Consumer Latency Histogram
    this.consumerProcessingDuration = new Histogram({
      name: 'kafka_consumer_processing_duration_seconds',
      help: 'End-to-end processing duration from poll to offset commit',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0],
    });

    this.consumerEventsConsumed = new Counter({
      name: 'kafka_consumer_events_consumed_total',
      help: 'Total events consumed',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
    });

    this.consumerRebalanceCount = new Counter({
      name: 'kafka_consumer_rebalance_total',
      help: 'Total number of consumer group rebalances',
      registers: [this.registry],
      labelNames: ['environment', 'stream'],
    });

    this.consumerDlqIngressCount = new Counter({
      name: 'kafka_consumer_dlq_ingress_total',
      help: 'Total events sent to Dead Letter Queue',
      registers: [this.registry],
      labelNames: ['environment', 'stream', 'reason'],
    });
  }

  public static initialize(registry: Registry): MetricsManager {
    if (!MetricsManager.instance) {
      MetricsManager.instance = new MetricsManager(registry);
    }
    return MetricsManager.instance;
  }

  public static getInstance(): MetricsManager {
    if (!MetricsManager.instance) {
      throw new Error('MetricsManager not initialized');
    }
    return MetricsManager.instance;
  }

  /**
   * Wraps an asynchronous Kafka producer operation with latency recording.
   */
  public async trackProducerOperation<T>(
    labels: MetricLabels,
    operation: () => Promise<T>
  ): Promise<T> {
    const validatedLabels = MetricLabelSchema.parse(labels);
    this.producerEventsAttempted.inc({ environment: validatedLabels.environment, stream: validatedLabels.stream });
    
    const start = process.hrtime.bigint();
    try {
      const result = await operation();
      const end = process.hrtime.bigint();
      
      this.producerProduceDuration.observe(
        { environment: validatedLabels.environment, stream: validatedLabels.stream },
        Number(end - start) / 1e9
      );
      this.producerEventsSuccess.inc({ environment: validatedLabels.environment, stream: validatedLabels.stream });
      return result;
    } catch (error) {
      this.producerEventsFailed.inc({ 
        environment: validatedLabels.environment, 
        stream: validatedLabels.stream,
        reason: error instanceof Error ? error.name : 'unknown'
      });
      throw error;
    }
  }

  /**
   * Records processing duration for a consumed event.
   */
  public recordConsumerLatency(labels: MetricLabels, startNs: bigint): void {
    const validatedLabels = MetricLabelSchema.parse(labels);
    const end = process.hrtime.bigint();
    this.consumerProcessingDuration.observe(
      { environment: validatedLabels.environment, stream: validatedLabels.stream },
      Number(end - startNs) / 1e9
    );
    this.consumerEventsConsumed.inc({ environment: validatedLabels.environment, stream: validatedLabels.stream });
  }

  /**
   * Tracks DLQ ingress.
   */
  public recordDlqIngress(labels: MetricLabels, reason: string): void {
    const validatedLabels = MetricLabelSchema.parse(labels);
    this.consumerDlqIngressCount.inc({
      environment: validatedLabels.environment,
      stream: validatedLabels.stream,
      reason,
    });
  }

  /**
   * Tracks consumer rebalances.
   */
  public recordRebalance(labels: MetricLabels): void {
    const validatedLabels = MetricLabelSchema.parse(labels);
    this.consumerRebalanceCount.inc({
      environment: validatedLabels.environment,
      stream: validatedLabels.stream,
    });
  }
}
