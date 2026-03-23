/**
 * @fileoverview Alerting and Flagging Subsystem.
 * Manages idempotent fraud event generation and high-priority Kafka publication.
 */

import { Redis } from 'ioredis';
import { KafkaMessagingClient } from '../../../../events/client/kafka_client';
import { TransactionFactory, TransactionFlagged, TransactionId, UserId, MerchantId, MonetaryAmount, Telemetry } from '../../../../core/domain_models';
import { MetricsCollector } from '../../../../utils/metrics/metrics-collector';

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
  private static instance: AlertingSubsystem;
  private readonly redis: Redis;
  private readonly kafkaClient: KafkaMessagingClient;
  private readonly metrics: MetricsCollector;
  private readonly config: AlertingConfig;

  private constructor(
    redis: Redis,
    kafkaClient: KafkaMessagingClient,
    metrics: MetricsCollector,
    config: AlertingConfig
  ) {
    this.redis = redis;
    this.kafkaClient = kafkaClient;
    this.metrics = metrics;
    this.config = config;
  }

  /**
   * Initializes or returns the singleton instance.
   */
  public static initialize(
    redis: Redis,
    kafkaClient: KafkaMessagingClient,
    metrics: MetricsCollector,
    config: AlertingConfig
  ): AlertingSubsystem {
    if (!AlertingSubsystem.instance) {
      AlertingSubsystem.instance = new AlertingSubsystem(redis, kafkaClient, metrics, config);
    }
    return AlertingSubsystem.instance;
  }

  public static getInstance(): AlertingSubsystem {
    if (!AlertingSubsystem.instance) {
      throw new Error('AlertingSubsystem not initialized');
    }
    return AlertingSubsystem.instance;
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
      environment: (process.env.NODE_ENV as 'development' | 'production' | 'test') || 'production',
      component: 'alerting_subsystem',
      stream_name: 'fraud_alerts',
    };

    try {
      // 1. Idempotency Check using atomic SETNX/SET PX
      const idempotencyKey = `fraud:alert:processed:${transactionId}`;
      const setSuccess = await this.redis.set(idempotencyKey, '1', 'PX', this.config.idempotencyTtlSeconds * 1000, 'NX');

      if (!setSuccess) {
        this.metrics.incrementThroughput(metricLabels, 'alert_deduplication_skipped');
        console.log(JSON.stringify({
          level: 'info',
          message: 'Alert deduplication skipped',
          transactionId,
          reason: 'Already processed'
        }));
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

      console.log(JSON.stringify({
        level: 'info',
        message: 'Transaction flagged successfully',
        transactionId,
        riskScore,
        reasons: reason,
        publicationStatus: 'success',
        orchestrationVersion,
        timestamp: Date.now()
      }));

    } catch (error) {
      this.metrics.incrementThroughput(metricLabels, 'kafka_producer_error');
      
      console.error(JSON.stringify({
        level: 'critical',
        message: 'Failed to dispatch fraud alert',
        transactionId,
        error: error instanceof Error ? error.message : 'Unknown error',
        stack: error instanceof Error ? error.stack : undefined,
        timestamp: Date.now()
      }));

      // Re-throw to signal orchestrator to abort transaction if necessary
      throw error;
    }
  }
}
