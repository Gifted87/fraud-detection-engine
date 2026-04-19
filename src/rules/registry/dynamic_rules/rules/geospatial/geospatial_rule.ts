import { Redis } from 'ioredis';
import { 
  FraudRule, 
  RuleEvaluationResult 
} from '../../contracts/fraud-rule-contract';
import { Transaction, Telemetry } from '../../../../../core/domain_models/definitions/transaction.interface';
import { MetricsCollector } from '../../../../../utils/metrics/metrics-collector';

/**
 * GeospatialRule implementation to detect impossible travel.
 * Calculates Haversine distance between sequential transactions to identify velocity violations.
 */
export class GeospatialRule implements FraudRule {
  public readonly ruleId: string = 'geospatial-rule-v1';
  public readonly description: string = 'Detects impossible travel patterns based on transaction velocity exceeding 900 km/h.';
  
  private readonly redis: Redis;
  private readonly metrics: MetricsCollector;
  private readonly SPEED_LIMIT_KMH = 900;

  /**
   * @param redisInstance Configured Redis instance for state lookup.
   * @param metrics Metrics collector for rule evaluation observability.
   */
  constructor(redisInstance: Redis, metrics: MetricsCollector) {
    this.redis = redisInstance;
    this.metrics = metrics;
  }

  /**
   * Evaluates if a transaction is suspicious based on geographical distance and time elapsed.
   * 
   * @param transaction The current transaction to evaluate.
   * @returns A RuleEvaluationResult indicating suspicion.
   */
  public async evaluate(transaction: Transaction): Promise<RuleEvaluationResult> {
    const startNs = process.hrtime.bigint();
    const redisKey = `user:${transaction.userId}:last_telemetry`;
    const metricLabels = {
      environment: process.env.NODE_ENV as 'development' | 'production' | 'test' || 'production',
      component: 'fraud_engine',
      stream_name: 'transactions'
    };

    try {
      const lastStateRaw = await this.redis.get(redisKey);
      
      // Update state for current transaction
      const currentState = {
        telemetry: {
          latitude: transaction.telemetry.latitude,
          longitude: transaction.telemetry.longitude,
        },
        timestamp: transaction.timestamp.toString()
      };
      await this.redis.set(redisKey, JSON.stringify(currentState));

      if (!lastStateRaw) {
        this.metrics.observeLatency(metricLabels, 'evaluate', startNs);
        return {
          isSuspicious: false,
          riskScore: 0.0,
          reason: 'No previous telemetry found for user (warm-up phase).'
        };
      }

      const lastState = JSON.parse(lastStateRaw);
      const lat1 = lastState.telemetry ? lastState.telemetry.latitude : lastState.latitude;
      const lon1 = lastState.telemetry ? lastState.telemetry.longitude : lastState.longitude;
      const currentTelemetry = transaction.telemetry;

      const distanceKm = this.calculateDistance(
        lat1,
        lon1,
        currentTelemetry.latitude,
        currentTelemetry.longitude
      );

      // Calculate time difference in hours. 
      // transaction.timestamp and lastState.timestamp are in nanoseconds (bigint).
      const lastTimestamp = BigInt(lastState.timestamp);
      const timeDiffNs = transaction.timestamp - lastTimestamp;
      
      // If time difference is negative or zero, we can't reliably calculate speed or it's an out-of-order event
      if (timeDiffNs <= 0n) {
          this.metrics.observeLatency(metricLabels, 'evaluate', startNs);
          return {
            isSuspicious: false,
            riskScore: 0.0,
            reason: 'Transaction is older than or same age as last recorded state (possible out-of-order event).'
          };
      }

      const timeElapsedHours = Number(timeDiffNs) / (1e9 * 3600);

      const speed = distanceKm / timeElapsedHours;
      const isSuspicious = speed > this.SPEED_LIMIT_KMH;
      
      console.log('DEBUG:', { distanceKm, timeElapsedHours, speed, isSuspicious });

      this.metrics.observeLatency(metricLabels, 'evaluate', startNs);
      
      return {
        isSuspicious,
        riskScore: isSuspicious ? 0.9 : 0.0,
        reason: isSuspicious 
          ? `Impossible travel detected: ${speed.toFixed(2)} km/h over ${distanceKm.toFixed(2)} km.` 
          : 'Geospatial velocity within normal bounds.'
      };
    } catch (error) {
      // Graceful degradation on error
      console.error(JSON.stringify({
        level: 'error',
        message: 'GeospatialRule evaluation error',
        ruleId: this.ruleId,
        transactionId: transaction.transactionId,
        error: error instanceof Error ? error.message : 'unknown'
      }));
      return {
        isSuspicious: false,
        riskScore: 0.0,
        reason: `Evaluation error: ${error instanceof Error ? error.message : 'unknown'}`
      };
    }
  }

  /**
   * Calculates the distance between two coordinates using the Haversine formula.
   */
  private calculateDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6371; // Earth radius in km
    const dLat = this.toRad(lat2 - lat1);
    const dLon = this.toRad(lon2 - lon1);
    
    const a = 
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(this.toRad(lat1)) * Math.cos(this.toRad(lat2)) * 
      Math.sin(dLon / 2) * Math.sin(dLon / 2);
    
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
  }

  private toRad(value: number): number {
    return value * Math.PI / 180;
  }
}
