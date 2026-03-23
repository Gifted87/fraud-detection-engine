import { Kafka, Producer, CompressionTypes } from 'kafkajs';
import { Registry, Counter, Histogram, Gauge } from 'prom-client';

/**
 * Custom error thrown when DLQ configuration is invalid or missing.
 */
export class DlqConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DlqConfigurationError';
  }
}

/**
 * Custom error thrown when a message fails to be published to the DLQ.
 */
export class DlqPublishError extends Error {
  constructor(message: string, public readonly originalError?: unknown) {
    super(message);
    this.name = 'DlqPublishError';
  }
}

/**
 * Interface for DLQ entry structure.
 */
export interface DlqEntry {
  readonly metadata: {
    readonly timestamp: number;
    readonly sourceComponent: string;
    readonly errorType: string;
    readonly originalTopic: string;
    readonly originalPartition: number;
    readonly originalOffset: string;
  };
  readonly payload: unknown;
  readonly signature: string | null;
}

/**
 * DLQHandler component for managing dead-letter queue operations.
 * Implements a singleton pattern with non-blocking async publishing.
 */
export class DlqHandler {
  private static instance: DlqHandler;
  private readonly producer: Producer;
  private readonly topic: string;
  private readonly metrics: {
    ingressTotal: Counter<string>;
    publishLatency: Histogram<string>;
    bufferLength: Gauge<string>;
  };
  private pendingCount = 0;

  private constructor(
    kafka: Kafka,
    dlqTopic: string,
    registry: Registry,
    sourceComponent: string
  ) {
    this.topic = dlqTopic;
    this.producer = kafka.producer({
      idempotent: true,
      maxInFlightRequests: 1,
      retry: {
        initialRetryTime: 300,
        retries: 8,
      },
    });

    this.metrics = {
      ingressTotal: new Counter({
        name: 'dlq_ingress_total',
        help: 'Total events routed to DLQ',
        registers: [registry],
        labelNames: ['error_type', 'source_component'],
      }),
      publishLatency: new Histogram({
        name: 'dlq_publish_latency_seconds',
        help: 'Latency of DLQ publish operations',
        registers: [registry],
        buckets: [0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
      }),
      bufferLength: new Gauge({
        name: 'dlq_buffer_length',
        help: 'Items currently in DLQ processing pipeline',
        registers: [registry],
      }),
    };
  }

  /**
   * Initializes the DLQHandler singleton.
   */
  public static initialize(
    kafka: Kafka,
    dlqTopic: string,
    registry: Registry,
    sourceComponent: string
  ): DlqHandler {
    if (!DlqHandler.instance) {
      DlqHandler.instance = new DlqHandler(kafka, dlqTopic, registry, sourceComponent);
    }
    return DlqHandler.instance;
  }

  public static getInstance(): DlqHandler {
    if (!DlqHandler.instance) {
      throw new DlqConfigurationError('DlqHandler not initialized');
    }
    return DlqHandler.instance;
  }

  /**
   * Connects the producer to the Kafka broker.
   */
  public async connect(): Promise<void> {
    await this.producer.connect();
  }

  /**
   * Publishes an event to the DLQ in a non-blocking manner.
   * 
   * @param payload Original event payload.
   * @param error Error object or string describing failure.
   * @param context Contextual info (topic, offset).
   * @param signature Cryptographic signature of the original message.
   */
  public async publish(
    payload: unknown,
    error: Error | string,
    context: { topic: string; partition: number; offset: string },
    signature: string | null = null
  ): Promise<void> {
    const errorType = error instanceof Error ? error.name : 'UnknownError';
    const startTime = process.hrtime.bigint();
    
    this.metrics.ingressTotal.labels(errorType, 'event_consumer').inc();
    this.pendingCount++;
    this.metrics.bufferLength.set(this.pendingCount);

    // Offload to promise pipeline without awaiting completion here
    this.executePublish(payload, error, context, signature, startTime).catch((err) => {
      console.error('Failed to publish to DLQ', err);
    });
  }

  private async executePublish(
    payload: unknown,
    error: Error | string,
    context: { topic: string; partition: number; offset: string },
    signature: string | null,
    startTime: bigint
  ): Promise<void> {
    try {
      const entry: DlqEntry = {
        metadata: {
          timestamp: Date.now(),
          sourceComponent: 'fraud-detection-engine',
          errorType: error instanceof Error ? error.name : 'UnknownError',
          originalTopic: context.topic,
          originalPartition: context.partition,
          originalOffset: context.offset,
        },
        payload,
        signature,
      };

      const key = `${context.offset}:${entry.metadata.errorType}`;

      await this.producer.send({
        topic: this.topic,
        messages: [{ key, value: JSON.stringify(entry) }],
        compression: CompressionTypes.GZIP,
      });

      const endTime = process.hrtime.bigint();
      this.metrics.publishLatency.observe(Number(endTime - startTime) / 1e9);
    } catch (err) {
      throw new DlqPublishError('Could not write to DLQ', err);
    } finally {
      this.pendingCount--;
      this.metrics.bufferLength.set(this.pendingCount);
    }
  }

  /**
   * Gracefully shuts down the producer, flushing all buffers.
   */
  public async shutdown(): Promise<void> {
    await this.producer.disconnect();
  }
}
