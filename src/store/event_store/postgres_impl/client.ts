import { Pool, PoolConfig } from 'pg';
import { z } from 'zod';

/**
 * Configuration schema for the PostgreSQL connection pool.
 * Enforces production-grade validation constraints.
 */
const PostgresConfigSchema = z.object({
  DB_URL: z.string().url(),
  DB_POOL_MIN: z.preprocess((val) => Number(val), z.number().int().positive()),
  DB_POOL_MAX: z.preprocess((val) => Number(val), z.number().int().positive()),
}).refine((data) => data.DB_POOL_MIN <= data.DB_POOL_MAX, {
  message: "DB_POOL_MIN cannot be greater than DB_POOL_MAX",
  path: ["DB_POOL_MIN"],
});

/**
 * Interface for pool statistics monitoring.
 */
export interface PoolStats {
  totalCount: number;
  idleCount: number;
  waitingCount: number;
}

/**
 * PostgresPoolManager handles the lifecycle of the database connection pool.
 * Implements a singleton pattern for application-wide connection management
 * in a high-throughput, event-sourced fraud detection engine.
 */
export class PostgresPoolManager {
  private static instance: PostgresPoolManager;
  private pool: Pool;
  private initialized: boolean = false;

  private constructor() {
    const rawConfig = {
      DB_URL: process.env.DB_URL,
      DB_POOL_MIN: process.env.DB_POOL_MIN,
      DB_POOL_MAX: process.env.DB_POOL_MAX,
    };

    const result = PostgresConfigSchema.safeParse(rawConfig);
    if (!result.success) {
      throw new Error(`Postgres Configuration Error: ${JSON.stringify(result.error.format())}`);
    }

    // Security constraint: Production environments MUST use SSL/TLS
    if (process.env.NODE_ENV === 'production' && !process.env.DB_URL?.includes('sslmode=verify-full')) {
      throw new Error('Insecure database connection detected: SSL/TLS with verify-full is mandatory in production.');
    }

    const config: PoolConfig = {
      connectionString: result.data.DB_URL,
      min: result.data.DB_POOL_MIN,
      max: result.data.DB_POOL_MAX,
      connectionTimeoutMillis: 5000,
      idleTimeoutMillis: 30000,
    };

    this.pool = new Pool(config);

    // Register error listener for robust operational monitoring
    this.pool.on('error', (err, client) => {
      console.error(JSON.stringify({
        level: 'error',
        message: 'Unexpected error on idle client',
        error: err.message,
        stack: err.stack,
        context: 'PostgresPoolManager'
      }));
    });
  }

  /**
   * Initializes the pool and performs a connectivity health check.
   * Provides a fail-fast mechanism for the persistence layer.
   */
  public async initialize(): Promise<void> {
    if (this.initialized) return;

    try {
      const client = await this.pool.connect();
      try {
        await client.query('SELECT 1');
      } finally {
        client.release();
      }
      this.initialized = true;
    } catch (err) {
      throw new Error(`Postgres health check failed during initialization: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /**
   * Returns the singleton instance of PostgresPoolManager.
   */
  public static getInstance(): PostgresPoolManager {
    if (!PostgresPoolManager.instance) {
      PostgresPoolManager.instance = new PostgresPoolManager();
    }
    return PostgresPoolManager.instance;
  }

  /**
   * Provides access to the underlying node-postgres Pool.
   */
  public getPool(): Pool {
    return this.pool;
  }

  /**
   * Returns current pool statistics for observability purposes.
   * Can be mapped to Prometheus metrics.
   */
  public getStats(): PoolStats {
    return {
      totalCount: this.pool.totalCount,
      idleCount: this.pool.idleCount,
      waitingCount: this.pool.waitingCount,
    };
  }

  /**
   * Redacted configuration logging for security compliance.
   * Ensures credentials are not exposed in logs.
   */
  public logConfiguration(): void {
    const dbUrl = process.env.DB_URL || '';
    // Redact password part of the connection string
    const redactedUrl = dbUrl.replace(/:([^@]+)@/, ':****@');
    console.info(JSON.stringify({
      level: 'info',
      message: 'Postgres connection pool configured',
      url: redactedUrl
    }));
  }
}
