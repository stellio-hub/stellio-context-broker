# Stellio Context Broker — Agent Guide

## Key Constraints

1. **Spec first.** Stellio implements ETSI TS 104 175 (NGSI-LD Core API) and TS 104 176 (NGSI-LD HTTP Binding). When spec and convenience conflict, spec wins.
2. **No blocking calls.** The stack is fully reactive (WebFlux + R2DBC + coroutines). Never block a reactive thread.
3. **No direct DB access from controllers.** Controllers → Services → Repositories only.
4. **No synchronous inter-service calls.** Use Kafka for all service-to-service communication.
5. **`shared` has no service dependencies.** It must not import `search-service` or `subscription-service`.
6. **Migrations are immutable.** Never modify a merged Flyway script — add a new one.
7. **Coverage minimum is 75% per module** (Jacoco). All new features need at least unit tests.

For any non-trivial change, run `/implement <feature>` (15-step checklist). PRs target `develop`.

## Toolchain Gotchas

### Jib multi-arch images

Jib builds two platform targets: `linux/amd64` and `linux/arm64`. If you add any native dependency (JNI,
platform-specific binary), verify that both platform builds pass before merging.

### Gradle parallelism caveat

Integration tests that use `JsonLdContextServerExtension` (WireMock-backed JSON-LD context server) must not run in
parallel within the same JVM. That is why `gradle.properties` sets `org.gradle.workers.max=1`. Do not remove this
constraint without verifying that the extension supports concurrent instances.
