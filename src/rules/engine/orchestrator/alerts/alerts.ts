/**
 * @fileoverview Alerting and Flagging Subsystem.
 * Manages idempotent fraud event generation and high-priority Kafka publication.
 */

import { Redis } from 'ioredis';
import { KafkaMessagingClient } from '../../../../events/client/kafka_client';
import { TransactionFactory, TransactionFlagged, TransactionId, UserId, MerchantId, MonetaryAmount, Telemetry } from '../../../../core/domain_models';
import { MetricsCollector } from '../../../../utils/metrics/metrics-collector';
import { Logger } from 'pino';

import { SystemConfiguration } from '../../../../core/domain_models/dependency_config';

interface Dependencies {
  redis: Redis;
  kafkaClient: KafkaMessagingClient;
  metricsCollector: MetricsCollector;
  logger: Logger;
  config: SystemConfiguration;
}

/**
 * Interface for Alerting subsystem configuration.
 */
export interface AlertingConfig {
  readonly idempotencyTtlSeconds: number;
}

/**
 * AlertingSubsystem handles idempotent fraud flagging and Kafka dispatch.
 */
export class AlertingSubsystem {
  private readonly redis: Redis;
  private readonly kafkaClient: KafkaMessagingClient;
  private readonly metrics: MetricsCollector;
  private readonly logger: Logger;
  private readonly config: AlertingConfig;
  private readonly environment: 'development' | 'production' | 'test';

  constructor({ redis, kafkaClient, metricsCollector, logger, config }: Dependencies) {
    this.redis = redis;
    this.kafkaClient = kafkaClient;
    this.metrics = metricsCollector;
    this.logger = logger;
    this.environment = config.NODE_ENV;
    this.config = { idempotencyTtlSeconds: 3600 };
  }



  /**
   * Dispatches a flagged transaction event to Kafka with idempotency guarantees.
   */
  public async dispatchFlag(
    transactionId: TransactionId,
    userId: UserId,
    merchantId: MerchantId,
    amount: MonetaryAmount,
    telemetry: Telemetry,
    reason: string,
    riskScore: number,
    orchestrationVersion: string
  ): Promise<void> {
    const startNs = process.hrtime.bigint();
    const metricLabels = {
      environment: this.environment,
      component: 'alerting_subsystem',
      stream_name: 'fraud_alerts',
    };

    try {
      // 1. Idempotency Check using atomic SETNX/SET PX
      const idempotencyKey = `fraud:alert:processed:${transactionId}`;
      const setSuccess = await this.redis.set(idempotencyKey, '1', 'PX', this.config.idempotencyTtlSeconds * 1000, 'NX');

      if (!setSuccess) {
        this.metrics.incrementThroughput(metricLabels, 'alert_deduplication_skipped');
        this.logger.info({ transactionId }, 'Alert deduplication skipped: already processed');
        return;
      }

      // 2. Construct Signed TransactionFlagged Event
      const envelope = await TransactionFactory.createTransactionFlagged(
        transactionId,
        userId,
        merchantId,
        amount,
        telemetry,
        reason,
        riskScore
      );

      // 3. Publish to Kafka
      await this.kafkaClient.publish('fraud-alerts-high-priority', envelope.payload);

      this.metrics.incrementThroughput(metricLabels, 'total_alerts_sent');
      this.metrics.observeLatency(metricLabels, 'dispatch_flag_success', startNs);

      this.logger.info({ 
        transactionId, 
        riskScore, 
        orchestrationVersion 
      }, 'Transaction flagged successfully');

    } catch (error) {
      // 4. Cleanup: Release the idempotency lock so that retries can successfully 
      //    attempt publication again if the failure was transient.
      const idempotencyKey = `fraud:alert:processed:${transactionId}`;
      await this.redis.del(idempotencyKey);

      this.metrics.incrementThroughput(metricLabels, 'kafka_producer_error');
      
      this.logger.error({ 
        transactionId, 
        error: error instanceof Error ? error.message : 'Unknown error' 
      }, 'Failed to dispatch fraud alert');

      // Re-throw to signal orchestrator to abort transaction if necessary
      throw error;
    }
  }
}
