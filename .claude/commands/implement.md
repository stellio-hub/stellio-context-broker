Walk through the full Stellio implementation checklist for: $ARGUMENTS

Work through these 15 steps in order. Do not skip any step. Confirm completion of each before moving to the next.

## Step 1 — Spec first
Identify the relevant ETSI GS CIM 009 section for this feature. Use the `ngsi-ld-spec` MCP (`get_spec_section` or
`search_spec`) to retrieve and quote the authoritative text before writing any code. State the section number explicitly.

## Step 2 — Module ownership
Identify which module(s) own this change (`api-gateway`, `search-service`, `subscription-service`, `shared`). Confirm
the change does not cross service boundaries (no synchronous inter-service calls, `shared` must not import service modules).

## Step 3 — Rules check
Read `.claude/rules/` and list every rule that applies to this change (architecture, api-design, kotlin-spring,
persistence, testing, git).

## Step 4 — Read existing code
Use Read and Grep to understand the existing implementation in the affected area. Do not write anything new until you
have read the relevant service, repository, controller, and model files.

## Step 5 — Flyway migration (if needed)
If this change requires a schema change:
1. Find the highest existing migration number: `find <module>/src/main/resources/db/migration -name 'V*.sql' | sort -t_ -k1 -V | tail -1`
2. Create the next script as `V<N+1>__<description>.sql` with idempotent DDL (`IF NOT EXISTS`, `IF EXISTS`, `CREATE OR REPLACE`).

## Step 6 — Service layer first
Implement business logic in the service class. Then wire up the controller. Never put business logic in controllers.

## Step 7 — Reactive correctness
Verify all new functions are `suspend` functions. No blocking calls (`runBlocking`, JDBC, synchronous HTTP).
Use R2DBC for DB access.

## Step 8 — Unit tests
Write MockK unit tests for the service layer. Target ≥ 75% line coverage. 
Follow naming: `[function] should [expected behavior] when [condition]`.

## Step 9 — Integration tests
If the change touches DB or Kafka: write Testcontainers integration tests. Do not mock repositories in integration tests.

## Step 10 — Run tests
```bash
./gradlew :<module>:test
```
All tests must pass before continuing.

## Step 11 — Detekt
```bash
./gradlew :<module>:detekt
```
Fix all new violations. Suppress only with documented rationale.

## Step 12 — Full build
```bash
./gradlew :<module>:build
```
Build must pass cleanly.

## Step 13 — Error responses
Confirm every error path uses an `APIException` subtype from `shared/src/main/kotlin/com/egm/stellio/shared/model/ApiExceptions.kt`
and produces a valid NGSI-LD `ProblemDetails` JSON response.

## Step 14 — Logging
Confirm log messages include entity ID and tenant where relevant. No sensitive data (tokens, credentials) is logged.

## Step 15 — Commit and PR
Write a Conventional Commit message (`feat:`, `fix:`, `refactor:`, etc.) that describes the *why*.
Open a PR against `develop`.
