import { DependencyContainer } from './dependency_config';

describe('DependencyContainer', () => {
  beforeEach(() => {
    // Reset singleton instance using reflection
    (DependencyContainer as any).instance = undefined;
  });

  it('should initialize successfully with valid environment variables', () => {
    const validEnv = {
      SIGNING_KEY: 'test-signing-key',
      ENCRYPTION_KEY: 'test-encryption-key',
      PUBLIC_KEY_PEM: 'test-public-key',
      PRIVATE_KEY_PEM: 'test-private-key',
      REDIS_URL: 'redis://localhost:6379',
      KAFKA_BROKERS: 'localhost:9092,localhost:9093',
      NODE_ENV: 'test'
    };

    const container = DependencyContainer.initialize(validEnv as NodeJS.ProcessEnv);
    expect(container).toBeDefined();

    const config = container.getConfig();
    expect(config.SIGNING_KEY).toBe('test-signing-key');
    expect(config.KAFKA_BROKER_LIST).toEqual(['localhost:9092', 'localhost:9093']);
    expect(config.ENVIRONMENT).toBe('test');
  });

  it('should throw an error if mandatory variables are missing', () => {
    const invalidEnv = {
      SIGNING_KEY: 'test-signing-key',
      // Missing other variables
    };

    expect(() => {
      DependencyContainer.initialize(invalidEnv as NodeJS.ProcessEnv);
    }).toThrow('Missing mandatory configuration variable: ENCRYPTION_KEY');
  });

  it('should format PEM keys by replacing literal newline text with actual newlines', () => {
    const env = {
      SIGNING_KEY: 'key',
      ENCRYPTION_KEY: 'key',
      PUBLIC_KEY_PEM: 'line1\\nline2',
      PRIVATE_KEY_PEM: 'line3\\nline4',
      REDIS_URL: 'redis',
      KAFKA_BROKERS: 'kafka',
      NODE_ENV: 'test'
    };

    const container = DependencyContainer.initialize(env as NodeJS.ProcessEnv);
    const config = container.getConfig();
    
    expect(config.PUBLIC_KEY_PEM).toBe('line1\nline2');
    expect(config.PRIVATE_KEY_PEM).toBe('line3\nline4');
  });
});
