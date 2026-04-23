import { Registry, Gauge } from 'prom-client';
import { KafkaConfigProvider } from './config/kafka-config';
import { KafkaEventProducer } from './producer/producer';
import { FraudEventConsumer } from './consumer/consumer';
import { MetricsManager } from './telemetry/metrics';
import { Transaction } from '../../../core/domain_models/definitions/transaction.interface';
import { Logger } from 'pino';

interface Dependencies {
  registry: Registry;
  logger: Logger;
}

/**
 * KafkaMessagingClient
 * 
 * Acts as the primary facade for the messaging subsystem. 
 * Orchestrates producers, consumers, and lifecycle management.
 */
export class KafkaMessagingClient {
  private readonly configProvider: KafkaConfigProvider;
  private readonly metrics: MetricsManager;
  private readonly producer: KafkaEventProducer;
  private readonly consumers: FraudEventConsumer[] = [];
  
  private readonly statusGauge: Gauge<string>;
  private readonly logger: Logger;
  private isRunning: boolean = false;

  constructor({ registry, logger }: Dependencies) {
    const env = process.env;
    this.logger = logger;
    this.metrics = MetricsManager.initialize(registry);
    this.configProvider = KafkaConfigProvider.initialize(env, registry);
    this.producer = KafkaEventProducer.initialize(this.configProvider, this.metrics);

    this.statusGauge = new Gauge({
      name: 'messaging_client_status',
      help: '1 if messaging client is running, 0 otherwise',
      registers: [registry],
      labelNames: ['component'],
    });
  }

  /**
   * Bootstraps the messaging subsystem.
   */
  public async start(): Promise<void> {
    if (this.isRunning) return;

    try {
      await this.producer.connect();
      this.statusGauge.labels('producer').set(1);
      
      for (const consumer of this.consumers) {
        await consumer.start();
        this.statusGauge.labels(`consumer`).set(1);
      }

      this.isRunning = true;
      this.producer.registerShutdownHandlers();
      
      process.on('SIGTERM', () => this.shutdown());
      process.on('SIGINT', () => this.shutdown());
    } catch (error) {
      this.statusGauge.labels('producer').set(0);
      throw new Error(`Failed to initialize KafkaMessagingClient: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * Publishes a transaction event to the specified topic.
   */
  public async publish<T extends Transaction>(topic: string, event: T): Promise<void> {
    if (!this.isRunning) {
      throw new Error('Messaging client is not running');
    }
    await this.producer.produce(topic, event);
  }

  /**
   * Registers a new consumer instance.
   */
  public registerConsumer(consumer: FraudEventConsumer): void {
    this.consumers.push(consumer);
  }

  /**
   * Performs a graceful shutdown of all components.
   */
  public async shutdown(): Promise<void> {
    if (!this.isRunning) return;

    try {
      await this.producer.gracefulShutdown();
      for (const consumer of this.consumers) {
        await consumer.shutdown();
      }
      this.statusGauge.labels('producer').set(0);
      this.statusGauge.labels('consumer').set(0);
      this.isRunning = false;
    } catch (error) {
      this.logger.error({ error }, 'Error during messaging client shutdown');
    }
  }
}
