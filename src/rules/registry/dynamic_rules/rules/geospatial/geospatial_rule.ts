import { Redis } from 'ioredis';
import { 
  FraudRule, 
  RuleEvaluationResult 
} from '../../contracts/fraud-rule-contract';
import { Transaction } from '../../../../../core/domain_models/definitions/transaction.interface';
import { MetricsCollector } from '../../../../../utils/metrics/metrics-collector';
import { Logger } from 'pino';

import { SystemConfiguration } from '../../../../../core/domain_models/dependency_config';

interface Dependencies {
  redis: Redis;
  metricsCollector: MetricsCollector;
  logger: Logger;
  config: SystemConfiguration;
}

/**
 * GeospatialRule implementation to detect impossible travel.
 * Calculates Haversine distance between sequential transactions to identify velocity violations.
 * 
 * Ensures chronological consistency: if out-of-order events arrive, the state is not 
 * corrupted with old data.
 */
export class GeospatialRule implements FraudRule {
  public readonly ruleId: string = 'geospatial-rule-v1';
  public readonly description: string = 'Detects impossible travel patterns based on transaction velocity exceeding 900 km/h.';
  
  private readonly redis: Redis;
  private readonly metrics: MetricsCollector;
  private readonly logger: Logger;
  private readonly SPEED_LIMIT_KMH = 900;
  private readonly environment: 'production' | 'development' | 'test';

  constructor({ redis, metricsCollector, logger, config }: Dependencies) {
    this.redis = redis;
    this.metrics = metricsCollector;
    this.logger = logger;
    this.environment = config.NODE_ENV;
  }

  public async evaluate(transaction: Transaction): Promise<RuleEvaluationResult> {
    const startNs = process.hrtime.bigint();
    const redisKey = `user:${transaction.userId}:last_telemetry`;
    const metricLabels = {
      environment: this.environment,
      component: 'fraud_engine',
      stream_name: 'transactions'
    };

    /**
     * Chronologically-consistent Atomic Read-Modify-Write using Lua script.
     * We use a Redis HASH to store telemetry and timestamp separately.
     * The script ONLY updates if the incoming transaction is newer than the stored state.
     */
    const luaScript = `
      local key = KEYS[1]
      local newTs = tonumber(ARGV[1])
      local newData = ARGV[2]
      
      local existingTsRaw = redis.call('HGET', key, 'ts')
      local existingTs = tonumber(existingTsRaw or "0")
      
      if newTs > existingTs then
        -- Incoming event is newer: capture existing state to return, then update.
        local lastData = redis.call('HGET', key, 'data')
        redis.call('HSET', key, 'ts', newTs, 'data', newData)
        return lastData
      else
        -- Out-of-order event: do not update state, and return CURRENT (newer) state 
        -- as the "last" for this message to prevent impossible travel jumps from old data.
        return redis.call('HGET', key, 'data')
      end
    `;

    const currentState = JSON.stringify({
      latitude: transaction.telemetry.latitude,
      longitude: transaction.telemetry.longitude,
      timestamp: transaction.timestamp.toString()
    });

    try {
      const lastStateRaw = await this.redis.eval(
        luaScript,
        1,
        redisKey,
        transaction.timestamp.toString(),
        currentState
      ) as string | null;

      if (!lastStateRaw) {
        this.metrics.observeLatency(metricLabels, 'evaluate', startNs);
        return {
          isSuspicious: false,
          riskScore: 0.0,
          reason: 'No previous telemetry found (warm-up phase).'
        };
      }

      const lastState = JSON.parse(lastStateRaw);
      const lat1 = lastState.latitude;
      const lon1 = lastState.longitude;
      const t1 = BigInt(lastState.timestamp);

      const lat2 = transaction.telemetry.latitude;
      const lon2 = transaction.telemetry.longitude;
      const t2 = transaction.timestamp;

      // Coordinate type and value safety (prevents NaN math)
      if (typeof lat1 !== 'number' || typeof lon1 !== 'number' || !isFinite(lat1) || !isFinite(lon1)) {
        throw new Error(`Inconsistent telemetry data stored: lat1=${lat1}, lon1=${lon1}`);
      }
      if (typeof lat2 !== 'number' || typeof lon2 !== 'number' || !isFinite(lat2) || !isFinite(lon2)) {
        throw new Error(`Invalid transaction telemetry coordinates: lat2=${lat2}, lon2=${lon2}`);
      }

      const distanceKm = this.calculateDistance(lat1, lon1, lat2, lon2);
      const timeDiffNs = t2 - t1;

      // Out-of-order or duplicate event — already handled by Lua but double check logic.
      if (timeDiffNs <= 0n) {
        this.metrics.observeLatency(metricLabels, 'evaluate', startNs);
        return {
          isSuspicious: false,
          riskScore: 0.0,
          reason: 'Transaction is older than or same age as last recorded state (out-of-order).'
        };
      }

      const timeElapsedHours = Number(timeDiffNs) / (1e9 * 3600);
      const speed = distanceKm / timeElapsedHours;
      const isSuspicious = speed > this.SPEED_LIMIT_KMH;

      this.metrics.observeLatency(metricLabels, 'evaluate', startNs);
      
      return {
        isSuspicious,
        riskScore: isSuspicious ? 0.9 : 0.0,
        reason: isSuspicious 
          ? `Impossible travel: ${speed.toFixed(2)} km/h over ${distanceKm.toFixed(2)} km.` 
          : 'Geospatial velocity within normal bounds.'
      };

    } catch (error) {
      this.logger.fatal({
        ruleId: this.ruleId,
        transactionId: transaction.transactionId,
        error: error instanceof Error ? error.message : 'unknown'
      }, 'GeospatialRule evaluation failure');
      throw error; 
    }
  }

  private calculateDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const R = 6371;
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
