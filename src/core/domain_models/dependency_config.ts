import { CryptoManager } from '../../utils/security/crypto';

/**
 * Interface defining the strict configuration schema for the application.
 * All environment variables must be mapped here.
 */
export interface SystemConfiguration {
  readonly SIGNING_KEY: string;
  readonly ENCRYPTION_KEY: string;
  readonly PUBLIC_KEY_PEM: string;
  readonly PRIVATE_KEY_PEM: string;
  readonly REDIS_URL: string;
  readonly KAFKA_BROKER_LIST: string[];
  readonly ENVIRONMENT: 'development' | 'production' | 'test';
}

/**
 * Registry for managing application dependencies and service lifecycles.
 */
export class DependencyContainer {
  private static instance: DependencyContainer;
  private readonly config: SystemConfiguration;
  private cryptoManagerInitialized: boolean = false;

  private constructor(config: SystemConfiguration) {
    this.config = Object.freeze(config);
  }

  /**
   * Validates and initializes the system configuration.
   * Ensures all required environment variables are present.
   */
  public static initialize(env: NodeJS.ProcessEnv): DependencyContainer {
    const requiredKeys = [
      'SIGNING_KEY',
      'ENCRYPTION_KEY',
      'PUBLIC_KEY_PEM',
      'PRIVATE_KEY_PEM',
      'REDIS_URL',
      'KAFKA_BROKERS',
      'NODE_ENV',
    ];

    for (const key of requiredKeys) {
      if (!env[key]) {
        throw new Error(`Missing mandatory configuration variable: ${key}`);
      }
    }

    const config: SystemConfiguration = {
      SIGNING_KEY: env.SIGNING_KEY!,
      ENCRYPTION_KEY: env.ENCRYPTION_KEY!,
      PUBLIC_KEY_PEM: env.PUBLIC_KEY_PEM!.replace(/\\n/g, '\n'),
      PRIVATE_KEY_PEM: env.PRIVATE_KEY_PEM!.replace(/\\n/g, '\n'),
      REDIS_URL: env.REDIS_URL!,
      KAFKA_BROKER_LIST: env.KAFKA_BROKERS!.split(','),
      ENVIRONMENT: (env.NODE_ENV as 'development' | 'production' | 'test') || 'production',
    };

    if (!DependencyContainer.instance) {
      DependencyContainer.instance = new DependencyContainer(config);
    }

    return DependencyContainer.instance;
  }

  /**
   * Returns the singleton instance of the dependency container.
   */
  public static getInstance(): DependencyContainer {
    if (!DependencyContainer.instance) {
      throw new Error('DependencyContainer not initialized. Call initialize() first.');
    }
    return DependencyContainer.instance;
  }

  /**
   * Initializes core services such as the CryptoManager.
   */
  public boot(): void {
    if (this.cryptoManagerInitialized) {
      return;
    }

    CryptoManager.initialize(
      this.config.ENCRYPTION_KEY,
      this.config.PUBLIC_KEY_PEM,
      this.config.PRIVATE_KEY_PEM
    );

    this.cryptoManagerInitialized = true;
  }

  public getConfig(): SystemConfiguration {
    return this.config;
  }
}
