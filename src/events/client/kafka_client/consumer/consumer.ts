import { Kafka, Consumer, EachBatchPayload } from 'kafkajs';
import { KafkaConfig, KafkaConfigProvider } from '../config/kafka-config';
import { MetricsManager } from '../telemetry/metrics';
import { EventEnvelopeFactory, MessageEnvelope } from '../../../../core/domain_models/messaging/event-envelope.schema';
import { TransactionSchema } from '../contracts/transaction.schemas';
import { ProjectionStore } from '../../../../store/projection_store/projection-store';
import { Transaction } from '../../../../core/domain_models/definitions/transaction.interface';

/**
 * Processor function type for custom business logic.
 */
export type MessageProcessor = (tx: Transaction) => Promise<void>;

/**
 * Production-grade Kafka Consumer for the Fraud Detection Engine.
 * Handles event ingestion, cryptographic verification, DLQ routing, and state projection.
 */
export class FraudEventConsumer {
  private readonly consumer: Consumer;
  private readonly kafka: Kafka;
  private readonly config: KafkaConfig;
  private readonly metrics: MetricsManager;
  private readonly projectionStore: ProjectionStore;

  constructor(
    private readonly topic: string,
    private readonly dlqTopic: string = 'fraud-dlq',
    private readonly processor?: MessageProcessor
  ) {
    this.config = KafkaConfigProvider.getInstance().getConfig();
    this.metrics = MetricsManager.getInstance();
    this.projectionStore = ProjectionStore.getInstance();

    this.kafka = new Kafka({
      clientId: this.config.clientId,
      brokers: this.config.brokers,
      sasl: this.config.sasl as any,
      ssl: this.config.ssl,
    });

    this.consumer = this.kafka.consumer({
      groupId: this.config.groupId,
      sessionTimeout: this.config.consumer.sessionTimeout,
      heartbeatInterval: this.config.consumer.heartbeatInterval,
    });
  }

  public async start(): Promise<void> {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: false });

    this.consumer.on('consumer.group_join', () => {
      this.metrics.recordRebalance({ environment: process.env.NODE_ENV || 'production', stream: this.topic });
    });

    await this.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async (payload: EachBatchPayload) => {
        const { batch, resolveOffset, heartbeat, commitOffsetsIfNecessary } = payload;
        const environment = process.env.NODE_ENV || 'production';

        for (const message of batch.messages) {
          const startTime = process.hrtime.bigint();
          
          try {
            if (!message.value) continue;

            const envelope = JSON.parse(message.value.toString(), (key, value) => {
              if (key === 'createdAtNs' || key === 'timestamp' || key === 'value') {
                return BigInt(value);
              }
              return value;
            }) as MessageEnvelope<Transaction>;

            // 1. Verify Integrity
            await EventEnvelopeFactory.verifyEnvelope(envelope);

            // 2. Validate Contract
            const validation = TransactionSchema.safeParse(envelope.payload);
            if (!validation.success) {
              throw new Error(`Schema validation failed: ${JSON.stringify(validation.error)}`);
            }

            // 3. Process Projection
            const tx = validation.data as Transaction;
            await this.projectionStore.processTransaction(
              tx.userId,
              tx.amount.value,
              tx.transactionId,
              60 // Default 60s sliding window
            );

            // 4. Custom processing (e.g., RuleEngine)
            if (this.processor) {
              await this.processor(tx);
            }

            // 5. Success metrics and offset handling
            this.metrics.recordConsumerLatency({ environment, stream: this.topic }, startTime);
            resolveOffset(message.offset);

          } catch (err) {
            const reason = err instanceof Error ? err.message : 'Unknown processing error';
            
            // Log to DLQ
            await this.kafka.producer().send({
              topic: this.dlqTopic,
              messages: [{
                key: message.key,
                value: JSON.stringify({
                  originalPayload: message.value?.toString(),
                  error: reason,
                  timestamp: Date.now()
                })
              }]
            });

            this.metrics.recordDlqIngress({ environment, stream: this.topic }, reason);
            resolveOffset(message.offset);
          }
          
          await heartbeat();
        }
        
        await commitOffsetsIfNecessary();
      },
    });
  }

  public async shutdown(): Promise<void> {
    await this.consumer.disconnect();
  }
}
