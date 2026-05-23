# Stellio Context Broker — Agent Guide

## 1. Project Overview

| Module                 | Role                                                        | Stack                                               |
|------------------------|-------------------------------------------------------------|-----------------------------------------------------|
| `api-gateway`          | Routing, auth, request validation                           | Spring Cloud Gateway                                |
| `search-service`       | Entity storage, temporal/geospatial queries, CSR federation | R2DBC + Flyway + Postgres (TimescaleDB + PostGIS)   |
| `subscription-service` | Subscriptions and notifications                             | R2DBC + Flyway + Postgres (PostGIS) + OAuth2 client |
| `shared`               | Shared library: models, utilities, test fixtures            | Kotlin library                                      |

**Stack:** Kotlin 2.3.10 · Spring Boot 4.0.x · Spring Cloud 2025.1.0 · Java 21 · WebFlux + coroutines · R2DBC · Kafka · TimescaleDB + PostGIS

---

## 2. Rules — Read These First

| File                                                               | Covers                                                                  |
|--------------------------------------------------------------------|-------------------------------------------------------------------------|
| [`.claude/rules/architecture.md`](.claude/rules/architecture.md)   | Module boundaries, reactive rules, CSR federation, security             |
| [`.claude/rules/api-design.md`](.claude/rules/api-design.md)       | NGSI-LD endpoints, HTTP status codes, error format, content negotiation |
| [`.claude/rules/kotlin-spring.md`](.claude/rules/kotlin-spring.md) | Formatting, naming, code structure, error handling, logging             |
| [`.claude/rules/persistence.md`](.claude/rules/persistence.md)     | R2DBC, Flyway migrations, jsonb, TimescaleDB hypertables                |
| [`.claude/rules/testing.md`](.claude/rules/testing.md)             | JUnit 5, MockK, Testcontainers, coverage minimums                       |
| [`.claude/rules/git.md`](.claude/rules/git.md)                     | Branch naming, Conventional Commits, pre-commit checklist               |

---

## 3. Key Constraints

1. **Spec first.** Stellio implements ETSI GS CIM 009. When spec and convenience conflict, spec wins.
2. **No blocking calls.** The stack is fully reactive (WebFlux + R2DBC + coroutines). Never block a reactive thread.
3. **No direct DB access from controllers.** Controllers → Services → Repositories only.
4. **No synchronous inter-service calls.** Use Kafka for all service-to-service communication.
5. **`shared` has no service dependencies.** It must not import `search-service` or `subscription-service`.
6. **Migrations are immutable.** Never modify a merged Flyway script — add a new one.
7. **Coverage minimum is 75% per module** (Jacoco). All new features need at least unit tests.

---

## 4. Quick Commands

```bash
# Build
./gradlew clean build                          # all modules
./gradlew :shared:build                        # single module

# Tests
./gradlew test                                 # all modules
./gradlew :shared:test                         # single module
./gradlew :shared:test --tests 'com.egm.stellio.shared.*'                              # single package
./gradlew :search-service:test --tests 'com.egm.stellio.search.entity.service.EntityServiceTest'  # single class
./gradlew :subscription-service:test --tests '*some method name*'                      # single method wildcard

# Static analysis
./gradlew detekt                               # all modules (or :module:detekt)

# Coverage reports (open in browser after ./gradlew test)
open search-service/build/reports/jacoco/test/html/index.html

# SonarCloud (requires SONAR_TOKEN)
./gradlew sonarqube

# Local dev (profile is set automatically)
./gradlew :search-service:bootRun              # SPRING_PROFILES_ACTIVE=dev

# Docker images (Jib)
./gradlew :api-gateway:jibDockerBuild :search-service:jibDockerBuild :subscription-service:jibDockerBuild
```

---

## 5. Local Development

### Environment variables (`.env` file, consumed by docker-compose)

```dotenv
# Postgres
POSTGRES_USER=stellio
POSTGRES_PASS=stellio_password
POSTGRES_DBNAME=stellio

# Per-service databases
STELLIO_SEARCH_DB_DATABASE=stellio_search
STELLIO_SUBSCRIPTION_DB_DATABASE=stellio_subscription

# Ports
API_GATEWAY_PORT=8080
SEARCH_SERVICE_PORT=8083
SUBSCRIPTION_SERVICE_PORT=8084

# Auth (set to false for local dev without Keycloak)
STELLIO_AUTHENTICATION_ENABLED=false
```

### Docker Compose

```bash
# Start full local stack (services + dependencies)
docker compose --env-file .env up -d

# Start only dependencies (Kafka + TimescaleDB/PostGIS Postgres)
docker compose -f docker-compose-dependencies.yml up -d
```

Services expose: gateway → `8080`, search → `8083`, subscription → `8084`.

When auth is disabled (`STELLIO_AUTHENTICATION_ENABLED=false`), alternate no-auth beans are activated
(e.g. `WebClientConfig.webClientNoAuthentification`). Keep both auth and no-auth code paths working.

---

## 6. Toolchain Details

*(Edge-case reference — consult when something breaks at the build/CI level.)*

### Kotlin compiler flags

Two extra flags are applied globally:
- `-Xconsistent-data-class-copy-visibility` — data class `copy()` respects the constructor's visibility
- JSR-305 strict (`-Xjsr305=strict`) — `@Nullable`/`@NonNull` annotations are enforced at compile time

### Jib multi-arch images

Jib builds two platform targets: `linux/amd64` and `linux/arm64`. Base image: `eclipse-temurin:21-jre`. If you add
any native dependency (JNI, platform-specific binary), verify that both platform builds pass before merging.

### Gradle parallelism caveat

Integration tests that use `JsonLdContextServerExtension` (WireMock-backed JSON-LD context server) must not run in
parallel within the same JVM. The Gradle test task is therefore configured with `workers.max=1` for the affected
modules. Do not remove this constraint without verifying that the extension supports concurrent instances.

### Jacoco report paths

Reports are generated automatically after each test run (`tasks.test finalizedBy jacocoTestReport`):
- HTML: `<module>/build/reports/jacoco/test/html/index.html`
- XML (consumed by SonarCloud): `<module>/build/reports/jacoco/test/jacocoTestReport.xml`

---

## 7. Numbered Task List (Agent Checklist)

When implementing any non-trivial change, work through these steps in order:

1. Identify the relevant NGSI-LD spec section (ETSI GS CIM 009) and re-read it before writing any code.
2. Confirm which module(s) own the change; verify it respects service boundaries.
3. Check `.claude/rules/` for constraints that apply to this change.
4. Read the existing code in the affected area before writing anything new.
5. Write or update Flyway migration scripts if any schema change is needed.
6. Implement the feature in the service layer first (business logic), then wire up the controller.
7. Use `suspend` functions throughout; do not introduce blocking calls.
8. Add or update unit tests (MockK). Target ≥ 75% line coverage.
9. Add integration tests (Testcontainers) for any new DB or Kafka interaction.
10. Run `./gradlew :module:test` and confirm all tests pass.
11. Run `./gradlew detekt` and fix any new violations (suppress only with documented rationale).
12. Run `./gradlew :module:build` to confirm the full build passes.
13. Verify error responses use `APIException` subtypes and match the NGSI-LD `ProblemDetails` format.
14. Confirm log messages include relevant IDs (entity ID, tenant) and contain no sensitive data.
15. Write a Conventional Commit message (`feat:`, `fix:`, `refactor:`, etc.) and open a PR against `develop`.
