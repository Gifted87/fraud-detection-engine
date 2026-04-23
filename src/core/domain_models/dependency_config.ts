import { createContainer, asValue, asClass, InjectionMode, AwilixContainer } from 'awilix';
import Redis from 'ioredis';
import { Registry } from 'prom-client';
import { Logger } from 'pino';
import { z } from 'zod';
import { logger } from '../../utils/logging/logger';
import { ProjectionStore } from '../../store/projection_store/projection-store';
import { EventRepository } from '../../store/event_store/postgres_impl/repository';
import { RuleRegistry } from '../../rules/registry/dynamic_rules/registry/rule-registry';
import { KafkaMessagingClient } from '../../events/client/kafka_client';
import { WeightedRiskAggregator } from '../../rules/engine/orchestrator/aggregator/aggregator';
import { AlertingSubsystem } from '../../rules/engine/orchestrator/alerts/alerts';
import { EngineCoreOrchestrator } from '../../rules/engine/orchestrator/core/engine';
import { OrchestrationMetricsCollector } from '../../rules/engine/orchestrator/metrics/metrics';
import { MetricsCollector } from '../../utils/metrics/metrics-collector';

// Rules
import { VelocityRule } from '../../rules/registry/dynamic_rules/rules/velocity/velocity-rule';
import { GeospatialRule } from '../../rules/registry/dynamic_rules/rules/geospatial/geospatial_rule';
import { MerchantBlacklistRule } from '../../rules/registry/dynamic_rules/rules/merchant/merchant-blacklist-rule';

/**
 * Zod schema for application configuration.
 * Enforces strict types and provides defaults for optional environment variables.
 */
const ConfigSchema = z.object({
  SIGNING_KEY: z.string().min(1, 'SIGNING_KEY is required'),
  ENCRYPTION_KEY: z.string().min(1, 'ENCRYPTION_KEY is required'),
  CRYPTO_SALT: z.string().min(1, 'CRYPTO_SALT is required'),
  PUBLIC_KEY_PEM: z.string().min(1).transform(v => v.replace(/\\n/g, '\n')),
  PRIVATE_KEY_PEM: z.string().min(1).transform(v => v.replace(/\\n/g, '\n')),
  REDIS_URL: z.string().default('redis://localhost:6379'),
  KAFKA_BROKERS: z.string().default('localhost:9092'),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('production'),
  FRAUD_THRESHOLD: z.coerce.number().default(0.8),
  METRICS_PORT: z.coerce.number().default(9090),
  RULE_WEIGHTS: z.record(z.string(), z.number()).default({
    'merchant-blacklist-rule-v1': 2.0,
    'geospatial-rule-v1': 1.5,
    'velocity-rule-v1': 1.0,
  }),
  CRITICAL_RULE_IDS: z.preprocess(
    (v) => (typeof v === 'string' ? v.split(',').map(s => s.trim()) : v),
    z.array(z.string())
  ).default(['merchant-blacklist-rule-v1', 'geospatial-rule-v1']),
  DB_URL: z.string().default('postgres://postgres:postgres@localhost:5432/fraud_engine'),
  DB_POOL_MIN: z.coerce.number().default(2),
  DB_POOL_MAX: z.coerce.number().default(10),
  MAX_CONSUMER_RETRIES: z.coerce.number().default(3),
  CONSUMER_CONCURRENCY: z.coerce.number().default(50),
  VELOCITY_THRESHOLD: z.coerce.number().default(10),
  VELOCITY_WINDOW_SECONDS: z.coerce.number().default(60),
  MERCHANT_BLACKLIST: z.string().default(''),
});

/**
 * Infers the configuration type from the Zod schema.
 */
export type SystemConfiguration = z.infer<typeof ConfigSchema>;

/**
 * Interface defining the cradle of dependencies available in the container.
 */
export interface Cradle {
  config: SystemConfiguration;
  registry: Registry;
  redis: Redis;
  logger: Logger;
  metricsCollector: MetricsCollector;
  projectionStore: ProjectionStore;
  eventRepository: EventRepository<any>;
  ruleRegistry: RuleRegistry;
  kafkaClient: KafkaMessagingClient;
  riskAggregator: WeightedRiskAggregator;
  orchestrationMetrics: OrchestrationMetricsCollector;
  alertingSubsystem: AlertingSubsystem;
  orchestrator: EngineCoreOrchestrator;
  
  // Rules
  velocityRule: VelocityRule;
  geospatialRule: GeospatialRule;
  merchantBlacklistRule: MerchantBlacklistRule;
}

/**
 * Factory function to create the system configuration from environment variables.
 */
export function createConfig(env: NodeJS.ProcessEnv): SystemConfiguration {
  const result = ConfigSchema.safeParse(env);
  
  if (!result.success) {
    const error = result.error.flatten();
    logger.error({ errors: error.fieldErrors }, 'Invalid application configuration');
    throw new Error(`Configuration validation failed: ${JSON.stringify(error.fieldErrors)}`);
  }

  return result.data;
}

/**
 * Awilix container setup for Dependency Injection.
 */
export async function createDependencyContainer(env: NodeJS.ProcessEnv): Promise<AwilixContainer<Cradle>> {
  const container = createContainer<Cradle>({
    injectionMode: InjectionMode.PROXY,
  });

  const config = createConfig(env);

  // 2. Register Values (Statics/Globals)
  const registry = new Registry();
  const redis = new Redis(config.REDIS_URL);
  
  container.register({
    config: asValue(config),
    registry: asValue(registry),
    redis: asValue(redis),
    logger: asValue(logger),
  });

  // 3. Register Classes (Stateful Services)
  container.register({
    metricsCollector: asValue(MetricsCollector.initialize(registry)),
    projectionStore: asValue(ProjectionStore.initialize(redis, registry)),
    eventRepository: asClass(EventRepository).singleton(),
    ruleRegistry: asClass(RuleRegistry).singleton(),
    kafkaClient: asClass(KafkaMessagingClient).singleton(),
    riskAggregator: asClass(WeightedRiskAggregator).singleton(),
    orchestrationMetrics: asValue(OrchestrationMetricsCollector.initialize(registry)),
    alertingSubsystem: asClass(AlertingSubsystem).singleton(),
    orchestrator: asClass(EngineCoreOrchestrator).singleton(),

    // Rules
    velocityRule: asClass(VelocityRule).singleton(),
    geospatialRule: asClass(GeospatialRule).singleton(),
    merchantBlacklistRule: asClass(MerchantBlacklistRule).singleton(),
  });

  return container;
}
