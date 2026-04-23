import { Kafka, Consumer, EachBatchPayload, Producer } from 'kafkajs';
import { KafkaConfig, KafkaConfigProvider } from '../config/kafka-config';
import { MetricsManager } from '../telemetry/metrics';
import { EventEnvelopeFactory, MessageEnvelope } from '../../../../core/domain_models/messaging/event-envelope.schema';
import { TransactionSchema, MessageEnvelopeSchema } from '../contracts/transaction.schemas';
import { ProjectionStore } from '../../../../store/projection_store/projection-store';
import { Transaction } from '../../../../core/domain_models/definitions/transaction.interface';
import { EventRepository } from '../../../../store/event_store/postgres_impl/repository';
import { Logger } from 'pino';
import { SystemConfiguration } from '../../../../core/domain_models/dependency_config';

interface Dependencies {
  eventRepository: EventRepository<Transaction>;
  projectionStore: ProjectionStore;
  logger: Logger;
  config: SystemConfiguration;
}

/** Processor function type for custom business logic. */
export type MessageProcessor = (tx: Transaction) => Promise<void>;

/**
 * Production-grade Kafka Consumer for the Fraud Detection Engine.
 */
export class FraudEventConsumer {
  private readonly consumer: Consumer;
  private readonly kafka: Kafka;
  private readonly kafkaConfig: KafkaConfig;
  private readonly metrics: MetricsManager;
  private readonly projectionStore: ProjectionStore;
  private readonly eventRepository: EventRepository<Transaction>;
  private readonly logger: Logger;
  private readonly systemConfig: SystemConfiguration;

  /** Single DLQ producer connected at startup and reused across all failures. */
  private dlqProducer: Producer | null = null;

  constructor(
    private readonly topic: string,
    private readonly dlqTopic: string = 'fraud-dlq',
    private readonly processor: MessageProcessor | undefined,
    { eventRepository, projectionStore, logger, config }: Dependencies
  ) {
    this.systemConfig = config;
    this.kafkaConfig = KafkaConfigProvider.getInstance().getConfig();
    this.metrics = MetricsManager.getInstance();
    this.projectionStore = projectionStore;
    this.eventRepository = eventRepository;
    this.logger = logger;

    this.kafka = new Kafka({
      clientId: this.kafkaConfig.clientId,
      brokers: this.kafkaConfig.brokers,
      sasl: this.kafkaConfig.sasl as any,
      ssl: this.kafkaConfig.ssl,
    });

    this.consumer = this.kafka.consumer({
      groupId: this.kafkaConfig.groupId,
      sessionTimeout: this.kafkaConfig.consumer.sessionTimeout,
      heartbeatInterval: this.kafkaConfig.consumer.heartbeatInterval,
    });
  }

  public async start(): Promise<void> {
    this.dlqProducer = this.kafka.producer();
    await this.dlqProducer.connect();

    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: false });

    this.consumer.on('consumer.group_join', () => {
      this.metrics.recordRebalance({
        environment: this.systemConfig.NODE_ENV,
        stream: this.topic,
      });
    });

    await this.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async (payload: EachBatchPayload) => {
        const { batch, resolveOffset, heartbeat, commitOffsetsIfNecessary } = payload;
        const environment = this.systemConfig.NODE_ENV;
        const messages = batch.messages;

        const keyedMessages = new Map<string, typeof messages>();
        for (const msg of messages) {
          const key = msg.key?.toString() ?? 'anonymous';
          if (!keyedMessages.has(key)) keyedMessages.set(key, []);
          keyedMessages.get(key)!.push(msg);
        }

        const groupPromises = Array.from(keyedMessages.values()).map(async (group) => {
          for (const message of group) {
            const startTime = process.hrtime.bigint();
            try {
              if (!message.value) {
                resolveOffset(message.offset);
                continue;
              }

              const rawData = JSON.parse(message.value.toString());
              let tx: Transaction;
              let envelope: MessageEnvelope<Transaction>;

              if (rawData.metadata && rawData.payload) {
                const envValidation = MessageEnvelopeSchema(TransactionSchema).safeParse(rawData);
                if (!envValidation.success) {
                  throw new Error(`Envelope validation failed: ${JSON.stringify(envValidation.error)}`);
                }
                envelope = envValidation.data as MessageEnvelope<Transaction>;
                tx = envelope.payload as Transaction;
              } else {
                const txValidation = TransactionSchema.safeParse(rawData);
                if (!txValidation.success) {
                  throw new Error(`Schema validation failed: ${JSON.stringify(txValidation.error)}`);
                }
                tx = txValidation.data as Transaction;
                envelope = await EventEnvelopeFactory.create(tx);
              }

              await EventEnvelopeFactory.verifyEnvelope(envelope);

              await this.retryWithBackoff(
                () => this.eventRepository.append(tx.userId, envelope),
                this.systemConfig.MAX_CONSUMER_RETRIES
              );

              await this.retryWithBackoff(
                () => this.projectionStore.processTransaction(
                  tx.userId,
                  tx.amount.value,
                  tx.transactionId,
                  this.systemConfig.VELOCITY_WINDOW_SECONDS
                ),
                this.systemConfig.MAX_CONSUMER_RETRIES
              );

              if (this.processor) {
                await this.processor(tx);
              }

              this.metrics.recordConsumerLatency({ environment, stream: this.topic }, startTime);

            } catch (err) {
              const reason = err instanceof Error ? err.message : 'Unknown processing error';
              await this.sendToDlq(message.key, message.value, reason);
              this.metrics.recordDlqIngress({ environment, stream: this.topic }, reason);
            }
          }
          await heartbeat();
        });

        await Promise.all(groupPromises);

        for (const message of messages) {
          resolveOffset(message.offset);
        }

        await commitOffsetsIfNecessary();
      },
    });
  }

  public async shutdown(): Promise<void> {
    await this.consumer.disconnect();
    if (this.dlqProducer) {
      await this.dlqProducer.disconnect();
    }
  }

  private async retryWithBackoff<T>(
    fn: () => Promise<T>,
    maxRetries: number
  ): Promise<T> {
    let attempt = 0;
    while (true) {
      try {
        return await fn();
      } catch (err) {
        if (attempt >= maxRetries) {
          throw err;
        }
        attempt++;
        const baseDelayMs = 100 * Math.pow(2, attempt - 1);
        const jitterMs = Math.floor(Math.random() * 50);
        const delayMs = baseDelayMs + jitterMs;

        this.logger.warn({
          attempt,
          maxRetries,
          delayMs,
          error: err instanceof Error ? err.message : String(err),
        }, 'Transient error — retrying with back-off');

        await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
      }
    }
  }

  private async sendToDlq(
    key: Buffer | null | undefined,
    value: Buffer | null | undefined,
    reason: string
  ): Promise<void> {
    if (!this.dlqProducer) return;

    try {
      await this.dlqProducer.send({
        topic: this.dlqTopic,
        messages: [
          {
            key,
            value: JSON.stringify({
              originalPayload: value?.toString(),
              error: reason,
              timestamp: Date.now(),
            }),
          },
        ],
      });
    } catch (dlqErr) {
      this.logger.fatal({
        dlqTopic: this.dlqTopic,
        originalError: reason,
        dlqError: dlqErr instanceof Error ? dlqErr.message : String(dlqErr),
      }, 'Failed to send message to DLQ');
      throw new Error(`Critical DLQ Failure: ${dlqErr instanceof Error ? dlqErr.message : String(dlqErr)}`);
    }
  }
}
