import { Registry } from 'prom-client';
import { DependencyContainer } from './core/domain_models/dependency_config';
import { CryptoValidator } from './core/domain_models/security/crypto-validator.service';
import { KafkaMessagingClient } from './events/client/kafka_client';
import { RuleRegistry } from './rules/registry/dynamic_rules/registry/rule-registry';
import { RuleEngine } from './rules/registry/dynamic_rules/orchestrator/rule-engine';
import { VelocityRule } from './rules/registry/dynamic_rules/rules/velocity/velocity-rule';
import { GeospatialRule } from './rules/registry/dynamic_rules/rules/geospatial/geospatial_rule';
import { MerchantBlacklistRule } from './rules/registry/dynamic_rules/rules/merchant/merchant-blacklist-rule';
import { ProjectionStore } from './store/projection_store/projection-store';
import { MetricsCollector } from './utils/metrics/metrics-collector';
import { FraudEventConsumer } from './events/client/kafka_client/consumer/consumer';
import { isTransactionValidated } from './core/domain_models/definitions/transaction.interface';
import Redis from 'ioredis';

/**
 * Main entry point for the Fraud Detection Engine.
 * Orchestrates the initialization of all subsystems and starts the event processing pipeline.
 */
async function bootstrap() {
  console.log('Starting Fraud Detection Engine initialization...');

  const registry = new Registry();
  
  // 1. Initialize Dependency Container with environment variables
  const container = DependencyContainer.initialize(process.env);
  container.boot();
  console.log('Dependency container initialized and booted.');
  
  const config = container.getConfig();

  // 2. Initialize Crypto Validator for HMAC signing
  CryptoValidator.initialize(config.SIGNING_KEY);
  console.log('Crypto validator initialized.');

  // 3. Initialize Metrics Collector
  const metricsCollector = MetricsCollector.initialize(registry);
  
  // 4. Initialize Redis and Projection Store for real-time state management
  const redis = new Redis(config.REDIS_URL);
  const projectionStore = ProjectionStore.initialize(redis, registry);
  console.log('Projection store initialized with Redis.');
  
  // 5. Initialize Rule Registry and register available fraud detection rules
  const ruleRegistry = RuleRegistry.initialize(registry);
  ruleRegistry.registerRule(new VelocityRule(projectionStore, registry));
  ruleRegistry.registerRule(new GeospatialRule(redis, metricsCollector));
  ruleRegistry.registerRule(new MerchantBlacklistRule(registry));
  console.log('Rule registry initialized and rules registered.');
  
  // 6. Initialize Kafka Client facade
  const kafkaClient = KafkaMessagingClient.getInstance(registry);
  
  // 7. Initialize Rule Engine (The central orchestrator)
  const ruleEngine = new RuleEngine(registry, ruleRegistry, kafkaClient);
  
  // 8. Register Consumers
  const validatedTransactionProcessor = async (tx: any) => {
    if (isTransactionValidated(tx)) {
      await ruleEngine.orchestrate(tx);
    }
  };

  const ruleEngineConsumer = new FraudEventConsumer(
    'transactions-validated',
    'fraud-dlq',
    validatedTransactionProcessor
  );
  
  kafkaClient.registerConsumer(ruleEngineConsumer);
  
  // 9. Start Kafka Client (Connects producers and consumers)
  await kafkaClient.start();
  
  console.log('Fraud Detection Engine started successfully and is listening for events.');
}

// Global error handler for the bootstrap process
bootstrap().catch(err => {
  console.error('CRITICAL: Failed to start Fraud Detection Engine:', err);
  process.exit(1);
});
