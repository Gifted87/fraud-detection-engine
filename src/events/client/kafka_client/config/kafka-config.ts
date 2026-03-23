import { z } from 'zod';
import { Registry } from 'prom-client';

/**
 * Zod schema for runtime validation of Kafka configuration.
 * Enforces production-grade security and tuning parameters.
 */
const KafkaConfigSchema = z.object({
  brokers: z.array(z.string()).min(1),
  clientId: z.string().min(1),
  groupId: z.string().min(1),
  sasl: z.object({
    mechanism: z.enum(['plain', 'scram-sha-256', 'scram-sha-512']),
    username: z.string().min(1),
    password: z.string().min(1),
  }).optional(),
  ssl: z.boolean().default(true),
  producer: z.object({
    idempotent: z.boolean().default(true),
    acks: z.union([z.literal('all'), z.literal(-1), z.literal(0), z.literal(1)]),
    maxInFlightRequests: z.number().int().min(1),
    batchSize: z.number().int().min(1024),
    lingerMs: z.number().int().min(0),
    retry: z.object({
      initialRetryTime: z.number().int(),
      retries: z.number().int(),
    }),
  }),
  consumer: z.object({
    sessionTimeout: z.number().int().min(6000),
    heartbeatInterval: z.number().int().min(2000),
    autoCommit: z.literal(false),
    maxBytesPerPartition: z.number().int().min(1048576),
  }),
});

export type KafkaConfig = z.infer<typeof KafkaConfigSchema>;

/**
 * KafkaConfigProvider manages loading, validating, and freezing
 * the Kafka configuration for the fraud detection pipeline.
 */
export class KafkaConfigProvider {
  private static instance: KafkaConfigProvider;
  private readonly config: KafkaConfig;
  private readonly metrics: Registry;

  private constructor(env: NodeJS.ProcessEnv, metrics: Registry) {
    this.metrics = metrics;

    const rawConfig = {
      brokers: (env.KAFKA_BROKERS || '').split(',').filter(Boolean),
      clientId: env.KAFKA_CLIENT_ID || 'fraud-detection-engine',
      groupId: env.KAFKA_GROUP_ID || 'fraud-detection-group',
      sasl: env.KAFKA_SASL_USERNAME
        ? {
            mechanism: (env.KAFKA_SASL_MECHANISM as any) || 'scram-sha-512',
            username: env.KAFKA_SASL_USERNAME,
            password: env.KAFKA_SASL_PASSWORD || '',
          }
        : undefined,
      ssl: env.NODE_ENV === 'production',
      producer: {
        idempotent: true,
        acks: 'all',
        maxInFlightRequests: 1,
        batchSize: 16384,
        lingerMs: 5,
        retry: {
          initialRetryTime: 300,
          retries: 8,
        },
      },
      consumer: {
        sessionTimeout: 10000,
        heartbeatInterval: 3000,
        autoCommit: false as const,
        maxBytesPerPartition: 1048576,
      },
    };

    const result = KafkaConfigSchema.safeParse(rawConfig);

    if (!result.success) {
      // Instrument validation failure
      this.metrics.getSingleMetric('config_validation_failure')?.set(1);
      throw new Error(`Kafka configuration validation failed: ${JSON.stringify(result.error.format())}`);
    }

    this.config = Object.freeze(result.data);
    this.metrics.getSingleMetric('config_validation_success')?.set(1);
  }

  /**
   * Initializes the KafkaConfigProvider with environment variables and registry.
   */
  public static initialize(env: NodeJS.ProcessEnv, metrics: Registry): KafkaConfigProvider {
    if (!KafkaConfigProvider.instance) {
      KafkaConfigProvider.instance = new KafkaConfigProvider(env, metrics);
    }
    return KafkaConfigProvider.instance;
  }

  /**
   * Retrieves the immutable configuration object.
   */
  public getConfig(): KafkaConfig {
    return this.config;
  }
}
