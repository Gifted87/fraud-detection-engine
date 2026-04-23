import type { Knex } from 'knex';
import * as dotenv from 'dotenv';

dotenv.config();

const config: { [key: string]: Knex.Config } = {
  development: {
    client: 'pg',
    connection: process.env.DB_URL || 'postgres://postgres:postgres@localhost:5432/fraud_engine',
    pool: {
      min: 2,
      max: 10
    },
    migrations: {
      tableName: 'knex_migrations',
      directory: './src/store/event_store/postgres_impl/migrations'
    }
  },
  production: {
    client: 'pg',
    connection: process.env.DB_URL,
    pool: {
      min: parseInt(process.env.DB_POOL_MIN || '2', 10),
      max: parseInt(process.env.DB_POOL_MAX || '10', 10)
    },
    migrations: {
      tableName: 'knex_migrations',
      directory: './src/store/event_store/postgres_impl/migrations'
    }
  }
};

export default config;
