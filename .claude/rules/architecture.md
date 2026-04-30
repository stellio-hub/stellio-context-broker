# Architecture Rules for Stellio Context Broker

## Spec compliance — the primary constraint

Stellio is an implementation of the NGSI-LD API specification
([ETSI GS CIM 009](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_CIM009v010901p.pdf)).
The spec is the authoritative reference for all API design, data model, and behavioral decisions. When the spec and
developer convenience conflict, the spec wins.

In practice this means:
- Endpoint paths, HTTP methods, request/response shapes, and query parameters are defined by the spec — do not invent
  them
- Error types, response headers (`NGSILD-Results-Count`, `NGSILD-Warning`, `Location`), and content types are
  spec-defined — match them exactly
- Entity structure, attribute types (Property, Relationship, GeoProperty), and temporal representations
  (Normalized, TemporalValues, Aggregated) are spec-defined — do not introduce custom shapes
- Federation and CSR behaviour (merge rules, warning propagation, mode semantics) follow the spec — check the relevant
  section before implementing any change

When implementing a new feature, locate the corresponding section in ETSI GS CIM 009 first, then translate it to code.
If a behaviour is ambiguous or unspecified in the current target version, raise it with the team before deciding.

## Modules

Stellio is a microservice-based NGSI-LD context broker composed of:
- api-gateway: Spring Cloud Gateway: routing, authentication, request validation
- search-service: entity storage, temporal queries, geospatial queries, CSR federation
- subscription-service: subscriptions and notifications
- shared: shared library (models, utilities, test fixtures)
- Kafka event streaming backbone
- TimescaleDB + PostGIS storage

## Mandatory Architecture Rules

- No direct database access from controllers — controllers call services
- Services contain business logic; repositories handle persistence only
- Inter-service communication must go through Kafka; no synchronous service-to-service calls
- Module `shared` must not depend on any service module
- All new features must respect service boundaries

## Reactive Architecture

- The project is fully reactive (WebFlux + R2DBC + coroutines)
- Do not introduce blocking calls in reactive flows
- New service code should use Kotlin `suspend` functions rather than raw `Mono`/`Flux` where possible
- Use non-blocking database (`r2dbc`) and Kafka APIs

## Context Source Registration (CSR) and Federation

Context Source Registrations follow NGSI-LD spec §5.2.9. A CSR describes an external broker that holds data for a set
of entity types/attributes and exposes one or more `operations`.

### Key model concepts

- `ContextSourceRegistration` — the registration record: `endpoint`, `mode` (INCLUSIVE or AUXILIARY), `information` 
(entity types + attributes covered), `operations`
- `Mode.INCLUSIVE` — remote data is merged on equal footing with local data
- `Mode.AUXILIARY` — remote data fills in only what the local broker does not have; processed last during merge
- `Operation` — controls which NGSI-LD operations the CSR participates in (e.g., `FEDERATION_OPS`, `RETRIEVE_OPS`)

### Distributed consumption pattern

When a query arrives (retrieve entity, query entities, temporal query):

1. `ContextSourceRegistrationService` selects CSRs matching the request filters (`CSRFilters`)
2. `DistributedEntityConsumptionService` (or `DistributedTemporalEntityConsumptionService` for temporal endpoints) fans
out HTTP calls to matching CSR endpoints **in parallel** using Arrow fx `parMap`
3. Each remote call returns `Either<NGSILDWarning, CompactedEntityWithCSR>` — failures produce a warning, not a hard error
4. `ContextSourceUtils.mergeEntities` / `mergeEntitiesLists` merges local and remote results:
   - Local data takes precedence for conflicting attributes
   - AUXILIARY CSRs are sorted last and only contribute missing attributes
   - Merge uses `IorNel<NGSILDWarning, Result>` to carry both partial results and warnings simultaneously
5. Warnings from unreachable or invalid CSRs are surfaced in the `NGSILD-Warning` HTTP response header — the overall
request never fails solely because a CSR is unavailable

### Rules for CSR code

- All remote HTTP calls go through `ContextSourceHttpUtils`; do not open `WebClient` calls directly in services
- Merge logic lives in `ContextSourceUtils`; keep it pure (no I/O, no Spring beans)
- Never propagate a CSR failure as a 5xx to the client — downgrade to a warning
- New distributed operations must follow the same fan-out → collect warnings → merge pattern

## Security

- Authentication: OAuth2/OIDC with Keycloak; token validation at the gateway
- Authorization: role-based access control via Spring Security authorities
- CORS: configured at gateway level
- For local dev without auth: `application.authentication.enabled=false` activates no-auth beans — keep both paths
working in new code
- Never hardcode credentials or tokens; use environment variables

## CI / Quality Gates

- CI: GitHub Actions — build, test, SonarCloud analysis, OWASP dependency check
- Detekt must pass on all subprojects (config: `config/detekt/detekt.yml`)
- SonarCloud quality gate must pass before merging
- Coverage minimum: 75% per module (enforced via Jacoco)
