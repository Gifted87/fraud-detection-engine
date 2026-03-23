import { Registry, Histogram, Counter } from 'prom-client';
import { RuleRegistry } from '../registry/rule-registry';
import { TransactionFactory } from '../../../../../core/domain_models';
import { KafkaMessagingClient } from '../../../../../events/client/kafka_client';
import { Transaction, TransactionValidated } from '../../../../../core/domain_models/definitions/transaction.interface';

/**
 * RuleEngine Orchestrator
 * 
 * The central decision-making hub of the fraud detection system.
 * Orchestrates concurrent execution of registered fraud rules and handles
 * downstream flagging events based on risk aggregation thresholds.
 */
export class RuleEngine {
  private readonly registry: Registry;
  private readonly ruleRegistry: RuleRegistry;
  private readonly kafkaClient: KafkaMessagingClient;
  
  private readonly evaluationLatency: Histogram<string>;
  private readonly totalProcessed: Counter<string>;
  private readonly totalFlagged: Counter<string>;
  private readonly totalErrors: Counter<string>;

  constructor(
    registry: Registry,
    ruleRegistry: RuleRegistry,
    kafkaClient: KafkaMessagingClient
  ) {
    this.registry = registry;
    this.ruleRegistry = ruleRegistry;
    this.kafkaClient = kafkaClient;

    this.evaluationLatency = new Histogram({
      name: 'fraud_engine_orchestration_latency_seconds',
      help: 'Latency of the entire orchestration pipeline (rule evaluation + aggregation)',
      registers: [this.registry],
      labelNames: ['environment'],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
    });

    this.totalProcessed = new Counter({
      name: 'fraud_engine_transactions_processed_total',
      help: 'Total number of transactions processed by the rule engine',
      registers: [this.registry],
      labelNames: ['environment'],
    });

    this.totalFlagged = new Counter({
      name: 'fraud_engine_transactions_flagged_total',
      help: 'Total number of transactions flagged as fraudulent',
      registers: [this.registry],
      labelNames: ['environment'],
    });

    this.totalErrors = new Counter({
      name: 'fraud_engine_orchestration_errors_total',
      help: 'Total number of orchestration-level errors',
      registers: [this.registry],
      labelNames: ['environment'],
    });
  }

  /**
   * Orchestrates the evaluation of a validated transaction.
   * 1. Evaluates all rules concurrently.
   * 2. Aggregates scores.
   * 3. Publishes flagging event if threshold is exceeded.
   */
  public async orchestrate(event: TransactionValidated): Promise<void> {
    const start = process.hrtime.bigint();
    const environment = process.env.NODE_ENV || 'production';
    
    try {
      this.totalProcessed.labels(environment).inc();

      // Parallel evaluation orchestrated by RuleRegistry
      const evaluation = await this.ruleRegistry.evaluateAll(event);

      // Structured Logging snapshot of decision process
      console.log(JSON.stringify({
        level: 'info',
        message: 'Transaction evaluation completed',
        transactionId: event.transactionId,
        isSuspicious: evaluation.isSuspicious,
        aggregateScore: evaluation.aggregateScore,
        results: evaluation.results,
        timestamp: Date.now()
      }));

      // Threshold logic & flagging process
      if (evaluation.isSuspicious) {
        await this.handleFlagging(event, evaluation.aggregateScore, evaluation.results.map(r => r.reason).join('; '));
        this.totalFlagged.labels(environment).inc();
      }

    } catch (error) {
      this.totalErrors.labels(environment).inc();
      console.error(JSON.stringify({
        level: 'critical',
        message: 'Orchestration pipeline failure',
        transactionId: event.transactionId,
        error: error instanceof Error ? error.message : 'Unknown error',
        timestamp: Date.now()
      }));
    } finally {
      const end = process.hrtime.bigint();
      const latency = Number(end - start) / 1e9;
      this.evaluationLatency.labels(environment).observe(latency);
    }
  }

  /**
   * Constructs and publishes a TransactionFlagged event to Kafka.
   */
  private async handleFlagging(
    event: TransactionValidated,
    riskScore: number,
    reason: string
  ): Promise<void> {
    const flaggedEvent = await TransactionFactory.createTransactionFlagged(
      event.transactionId,
      event.userId,
      event.merchantId,
      event.amount,
      event.telemetry,
      reason,
      riskScore
    );

    await this.kafkaClient.publish('transactions-flagged', flaggedEvent.payload);
  }
}
