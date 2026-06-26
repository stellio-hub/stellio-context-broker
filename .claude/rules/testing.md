# Testing Rules

## Frameworks

- JUnit 5 (`@Test`, `@BeforeEach`, etc.)
- MockK (not Mockito) for mocking; `springmockk` for Spring integration
- Testcontainers for PostgreSQL and Kafka in integration tests

## Test naming

Two conventions, chosen by the tier of the test. Never use the subject-less `it should …` form.

- **Unit / service / util tests** — name after the function under test:
  `<functionName> should <expected behavior> [when <condition>]`
  - Example: `mergeTemporalEntities should merge instances when datasetIds match`
  - The prefix is the literal Kotlin function being exercised (camelCase), so tests are greppable
    from the production symbol.
- **Controller / integration tests** — name after the HTTP operation under test: `<operation> should <result>`
  - Example: `create entity should return a 400 if entity does not have an type`
  - Use the name of the operation; describe the observable HTTP outcome.

Banned forms (migrate on sight):
- `it should …` / `it shouldn't …` — subject-less BDD style; replace with the function- or
  endpoint-prefixed form above.
- Names with no `should` at all — restate the expected behavior with `should`.
- Never abbreviate negation as `shouldn't` — always write `should not`.

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
