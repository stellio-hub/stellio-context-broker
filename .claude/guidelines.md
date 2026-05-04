Stellio Context Broker – Developer Guidelines (Project-Specific)

Audience: Advanced Kotlin/Spring developer contributing to stellio-context-broker.

# Build and Configuration

- Toolchain
  - Java 21 (Temurin recommended). Gradle wrapper is provided; do not downgrade toolchains.
  - Kotlin 2.3.10 across modules; the compiler uses JVM target 21. JSR-305 is strict and data class copy visibility is
  consistent (-Xconsistent-data-class-copy-visibility).
  - Spring Boot 4.0.x; Spring Cloud BOM 2025.1.0.
- Modules
  - Modules:
    - shared (library)
    - api-gateway (Spring Cloud Gateway)
    - search-service (R2DBC + Flyway + Postgres with Timescale and PostGIS extensions)
    - subscription-service (R2DBC + Flyway + Postgres with PostGIS extension + OAuth2 client)
    - Root settings: include("shared", "api-gateway", "search-service", "subscription-service").
- Build tasks
  - Build all: `./gradlew clean build`
  - Module build: `./gradlew :shared:build` (or any subproject)
  - Assemble library (shared): produces JAR named stellio-context-broker-shared.
  - Jacoco reports are generated automatically after tests per module (tasks.test finalizedBy jacocoTestReport). 
  Reports at *build/reports/jacoco/test*.
- Static analysis
  - Detekt is applied to all subprojects.
    - Config: *config/detekt/detekt.yml*; per-module baselines at *<module>/config/detekt/baseline.xml*.
    - Run: `./gradlew detekt` (or `:module:detekt`). Formatting rules via detekt-formatting 1.23.8.
- Container images via Jib
  - Jib configured in services; multi-arch images (linux/arm64, linux/amd64). Base image eclipse-temurin:21-jre.
  - Build service image: `./gradlew :search-service:jibDockerBuild` (similar for other modules: :api-gateway and 
  :subscription-service). Image tags default to stellio/...:latest-dev on the "develop" branch.
- Runtime profile and env
  - Tests run with `SPRING_PROFILES_ACTIVE=test` (set in Gradle). Local dev uses bootRun with `SPRING_PROFILES_ACTIVE=dev` 
  for services.
  - docker-compose.yml expects environment overrides (see .env pattern):
    - Postgres creds: `POSTGRES_USER`, `POSTGRES_PASS`, `POSTGRES_DBNAME`.
    - Database names per service: `STELLIO_SEARCH_DB_DATABASE`, `STELLIO_SUBSCRIPTION_DB_DATABASE`.
    - Ports: `API_GATEWAY_PORT`, `SEARCH_SERVICE_PORT`, `SUBSCRIPTION_SERVICE_PORT`.
    - Auth: `STELLIO_AUTHENTICATION_ENABLED` and tenant mapping (APPLICATION_TENANTS_0_*).
  - Dependencies compose file: *docker-compose-dependencies.yml* starts Kafka (confluentinc/cp-kafka) and a Timescale/PostGIS Postgres.

# Testing

- Framework stack
  - JUnit 5 (useJUnitPlatform across subprojects).
  - Mocking: springmockk and mockk. The shared testFixtures module excludes mockito in favour of mockk.
  - Testcontainers: service tests use Postgres and Kafka; Docker must be available for any @Test that brings up containers.
  - Gradle test task sets JVM arg `-Dmockk.junit.extension.checkUnnecessaryStub=true` to help detect stale stubs.
- Running tests
  - Entire repo: `./gradlew test`
  - Per module: `./gradlew :shared:test`
  - Single package/class/test:
    - `./gradlew :shared:test --tests 'com.egm.stellio.shared.*'`
    - `./gradlew :search-service:test --tests 'com.egm.stellio.search.entity.service.EntityServiceTest'`
    - `./gradlew :subscription-service:test --tests '*some method name*'`
- Adding tests
  - Prefer pure unit tests in shared to avoid bringing up Spring context when not needed.
  - For services, if Postgres or Kafka are required, use Testcontainers. Reuse fixtures from shared testFixtures to 
  avoid duplication: testImplementation(testFixtures(project(":shared"))).
  - Use Spring profiles to isolate config for tests. The build already sets `SPRING_PROFILES_ACTIVE=test`; keep any 
  test-specific *application-test.yml* under src/test/resources if needed.
- Coverage
  - Jacoco reports are automatically generated after tests: *build/reports/jacoco/test/html* per module.

# Additional Notes

- JSON-LD processing uses Titanium (`com.apicatalog:titanium-json-ld`) and Glassfish JSON. Cache and reuse contexts
  where applicable — see `shared/src/main/kotlin/com/egm/stellio/shared/util/JsonLdUtils.kt`.
- For local dev without auth, set `application.authentication.enabled=false`; this activates alternate beans
  (e.g., `WebClientConfig.webClientNoAuthentification`). Keep both auth and no-auth paths working in new code.
- To run the full local stack: `docker compose --env-file .env up -d`. Services expose 8080 (gateway), 8083 (search),
  8084 (subscription). Dependencies (Kafka, Postgres/TimescaleDB) are in `docker-compose-dependencies.yml`.
- SonarCloud runs in CI; local analysis: `./gradlew sonarqube` (requires token).
- Jib builds linux/amd64 and linux/arm64 images. If you add native dependencies, verify both platform builds pass.

# Quick Commands Reference

- Build all modules: `./gradlew clean build`
- Run tests (all/modules/one): `./gradlew test` | `./gradlew :shared:test` | `./gradlew :search-service:test --tests 'com.egm.*MyTest'`
- Static analysis: `./gradlew detekt`
- Coverage reports: open *build/reports/jacoco/test/html/index.html* under each module
- Run services in dev: `./gradlew :search-service:bootRun` (`SPRING_PROFILES_ACTIVE=dev` is set by the task)
- Build Docker images: `./gradlew :api-gateway:jibDockerBuild :search-service:jibDockerBuild :subscription-service:jibDockerBuild`

Notes
- Keep this file project-specific and up to date when adding modules, changing toolchains, or modifying CI/build behaviour.
