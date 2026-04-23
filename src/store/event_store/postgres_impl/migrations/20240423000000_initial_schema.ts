import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.raw('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"');

  await knex.schema.createTable('events', (table) => {
    table.uuid('event_id').primary().defaultTo(knex.raw('uuid_generate_v4()'));
    table.uuid('aggregate_id').notNullable();
    table.bigInteger('version').notNullable();
    table.string('event_type', 255).notNullable();
    table.jsonb('metadata').notNullable();
    table.jsonb('payload').notNullable();
    table.text('signature').notNullable();
    table.timestamp('created_at', { useTz: true }).notNullable().defaultTo(knex.fn.now());

    table.unique(['aggregate_id', 'version']);
  });

  // Indexes
  await knex.raw('CREATE INDEX idx_events_payload_gin ON events USING GIN (payload)');
  await knex.raw('CREATE INDEX idx_events_aggregate_sequence ON events (aggregate_id, version)');
  await knex.raw('CREATE INDEX idx_events_created_at_brin ON events USING BRIN (created_at)');
  
  // Comments
  await knex.raw("COMMENT ON TABLE events IS 'Immutable transaction log for event sourcing'");
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('events');
}
