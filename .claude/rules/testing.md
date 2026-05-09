# Testing Rules

## Frameworks

- JUnit 5 (`@Test`, `@BeforeEach`, etc.)
- MockK (not Mockito) for mocking; `springmockk` for Spring integration
- Testcontainers for PostgreSQL and Kafka in integration tests

## Test naming

- Unit tests: `[function] should [expected behavior] when [condition]`
- Integration tests: `[function] should [test scenario]`

## Coverage

- Minimum line coverage: **75%**
- All new features must at least include unit tests
- Critical paths (auth, merge logic, entity resolution, etc.) must have branch coverage

## Integration tests

- Required for: database access, Kafka producers and consumers
- Do **not** mock repositories in integration tests — use Testcontainers
- Reuse fixtures from `shared` testFixtures
- Keep test-specific config in `src/test/resources/application-test.yml`

## What not to mock

- Prefer real integration tests over excessive mocking
- Do not mock the database or Kafka broker in integration tests
- Only mock external HTTP calls (e.g. CSR endpoints) using `WireMock`
