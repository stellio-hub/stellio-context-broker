Create a Flyway migration script for: $ARGUMENTS

## Step 1 — Identify target module
Determine which module needs the migration (`search-service` or `subscription-service`). If not specified in the
arguments, ask before proceeding.

## Step 2 — Find the next migration number
Run:
```bash
find <module>/src/main/resources/db/migration -name 'V*.sql' | sort -t_ -k1 -V | tail -3
```
Extract the highest `N` from the `V<N>__` prefix. The new script must use `N+1`. Check for gaps — if the sequence
has a gap (e.g. V5 then V8), warn the user before creating the file.

## Step 3 — Name the file
Format: `V<N+1>__<short_snake_case_description>.sql`  
Place it in: `<module>/src/main/resources/db/migration/`

Example: `V42__add_temporal_query_index.sql`

## Step 4 — Scaffold the script
Create the file with:
- A header comment stating what this migration does and why (the context a future developer needs, not just what)
- Idempotent DDL only: use `IF NOT EXISTS`, `IF EXISTS`, `CREATE OR REPLACE`, `ADD COLUMN IF NOT EXISTS`, etc.
- No DML that could fail on re-run (wrap data migrations in existence checks)

## Step 5 — Remind about immutability
State clearly: **once this script is merged, it must never be modified**. Any correction requires a new migration script.

## Step 6 — Suggest next steps
Point to step 10 of `/project:implement` — run `./gradlew :<module>:test` to verify the migration applies cleanly
via Testcontainers.
