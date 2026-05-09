# Persistence Rules

## Source of truth

PostgreSQL + TimescaleDB is the source of truth. All DB access goes through the service layer — never directly from controllers or handlers.

## R2DBC

- Use R2DBC exclusively; never introduce blocking JDBC calls
- Wrap operations that need atomicity in `@Transactional`; on suspend functions this works with the R2DBC transaction manager — no extra configuration needed
- Repositories return `suspend` functions; do not return raw `Mono`/`Flux` in new code

## Flyway migrations

- All schema changes must ship with a Flyway migration script
- Naming convention: `V<N>__<short_description>.sql` where `<N>` is the next integer (no gaps, no reuse)
- Migration scripts live in `src/main/resources/db/migration/` within each service module
- Scripts must be idempotent where possible (use `IF NOT EXISTS`, `IF EXISTS`, `CREATE OR REPLACE`)
- Never modify an already-merged migration script — add a new one instead

## jsonb

- Columns storing NGSI-LD payloads use the native PostgreSQL `jsonb` type
- Use native jsonb operators in SQL (`->`, `->>`, `@>`, `?`, `#>>`) rather than casting to text or pulling into application memory

## TimescaleDB

- The `attribute_instance` and `attribute_instance_audit` tables in search-service are TimescaleDB hypertables partitioned on time
- Do not alter their chunk interval or retention policy without a coordinated migration and team review
- New time-series data must go into these tables, not plain Postgres tables

## Testing

- Use Testcontainers (Postgres + TimescaleDB image) for all integration tests that touch the DB
- Do not mock repositories in integration tests — run against a real container
