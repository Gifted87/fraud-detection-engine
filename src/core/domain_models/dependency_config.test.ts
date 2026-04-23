import { createDependencyContainer } from './dependency_config';

describe('createDependencyContainer', () => {
  it('should initialize successfully with valid environment variables', async () => {
    const validEnv = {
      SIGNING_KEY: 'test-signing-key',
      ENCRYPTION_KEY: 'test-encryption-key',
      CRYPTO_SALT: 'test-salt',
      PUBLIC_KEY_PEM: 'test-public-key',
      PRIVATE_KEY_PEM: 'test-private-key',
      REDIS_URL: 'redis://localhost:6379',
      KAFKA_BROKERS: 'localhost:9092,localhost:9093',
      NODE_ENV: 'test',
      FRAUD_THRESHOLD: '0.8',
      METRICS_PORT: '9090'
    };

    const container = await createDependencyContainer(validEnv as NodeJS.ProcessEnv);
    expect(container).toBeDefined();

    const config = container.resolve('config');
    expect(config.SIGNING_KEY).toBe('test-signing-key');
    expect(config.NODE_ENV).toBe('test');
    expect(config.FRAUD_THRESHOLD).toBe(0.8);
  });

  it('should format PEM keys by replacing literal newline text with actual newlines', async () => {
    const env = {
      SIGNING_KEY: 'key',
      ENCRYPTION_KEY: 'key',
      CRYPTO_SALT: 'salt',
      PUBLIC_KEY_PEM: 'line1\\nline2',
      PRIVATE_KEY_PEM: 'line3\\nline4',
      REDIS_URL: 'redis',
      KAFKA_BROKERS: 'kafka',
      NODE_ENV: 'test'
    };

    const container = await createDependencyContainer(env as NodeJS.ProcessEnv);
    const config = container.resolve('config');
    
    expect(config.PUBLIC_KEY_PEM).toBe('line1\nline2');
    expect(config.PRIVATE_KEY_PEM).toBe('line3\nline4');
  });
});
