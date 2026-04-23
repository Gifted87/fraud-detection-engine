import * as dotenv from 'dotenv';
import fastify from 'fastify';
import { createDependencyContainer } from './core/domain_models/dependency_config';
import { FraudEventConsumer } from './events/client/kafka_client/consumer/consumer';
import { isTransactionValidated } from './core/domain_models/definitions/transaction.interface';
import { logger } from './utils/logging/logger';

async function bootstrap() {
  logger.info('Starting Fraud Detection Engine initialization...');

  // 1. Initialize Dependency Injection Container
  const container = await createDependencyContainer(process.env);
  
  const { 
    registry, 
    orchestrator, 
    kafkaClient, 
    ruleRegistry, 
    redis,
    eventRepository,
    config,
    velocityRule,
    geospatialRule,
    merchantBlacklistRule
  } = container.cradle;

  // 2. Register Rules into the Registry
  ruleRegistry.registerRule(velocityRule);
  ruleRegistry.registerRule(geospatialRule);
  ruleRegistry.registerRule(merchantBlacklistRule);

  // 3. Define the processing pipeline
  const validatedTransactionProcessor = async (tx: any) => {
    if (isTransactionValidated(tx)) {
      await orchestrator.orchestrate(tx);
    }
  };

  const ruleEngineConsumer = new FraudEventConsumer(
    'transactions-validated',
    'fraud-dlq',
    validatedTransactionProcessor,
    eventRepository
  );
  
  kafkaClient.registerConsumer(ruleEngineConsumer);

  // 4. Start Messaging Subsystem
  await kafkaClient.start();

  // 5. Metrics Exposure Server (Fastify)
  const server = fastify();
  server.get('/metrics', async (request, reply) => {
    reply.type('text/plain').send(await registry.metrics());
  });

  try {
    await server.listen({ port: config.METRICS_PORT, host: '0.0.0.0' });
    logger.info({ port: config.METRICS_PORT }, 'Metrics server listening');
  } catch (err) {
    logger.error({ err }, 'Failed to start metrics server');
    process.exit(1);
  }

  // 6. Lifecycle Management (Hot-reload & Shutdown)
  process.on('SIGHUP', () => {
    logger.info({ pid: process.pid }, 'SIGHUP received — re-parsing .env and hot-reloading configurations');
    try {
      dotenv.config({ override: true });
      ruleRegistry.reloadAll();
    } catch (err) {
      logger.error({ err: err instanceof Error ? err.message : String(err) }, 'Hot-reload failed');
    }
  });

  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Shutting down gracefully');
    try {
      await kafkaClient.shutdown();
      await server.close();
      redis.disconnect();
    } catch (err) {
      logger.error({ err }, 'Error during graceful shutdown');
    }
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  
  logger.info({ pid: process.pid }, 'Fraud Detection Engine started successfully.');
}

bootstrap().catch(err => {
  logger.fatal({ 
    message: 'CRITICAL: Failed to start Fraud Detection Engine',
    err: err instanceof Error ? err.message : String(err) 
  });
  process.exit(1);
});
