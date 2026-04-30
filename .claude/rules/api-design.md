# API Design Rules

Stellio implements the NGSI-LD API
([ETSI GS CIM 009](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.09.01_60/gs_CIM009v010901p.pdf)).
Every decision in this file derives from that spec. Before adding or changing any endpoint, consult the relevant spec
section — do not infer behaviour from REST conventions alone. The sections below document what the spec mandates so 
agents don't need to re-derive it each time.

## REST Conventions

- **Naming**: Nouns, not verbs — `/entities`, `/subscriptions`, `/csourceRegistrations`
- **HTTP Methods**: GET (read), POST (create), PATCH (update), DELETE (delete); use 405 for unsupported methods
- **HTTP Status Codes**:
  - `200 OK` — successful GET or PATCH with response body
  - `201 Created` — successful POST; include `Location` header with new resource URI
  - `204 No Content` — successful DELETE or PATCH with no body
  - `207 Multi-Status` — batch or distributed operations with mixed results
  - `400 Bad Request` — invalid input (malformed JSON-LD, missing required field)
  - `401 Unauthorized` — missing or invalid token
  - `403 Forbidden` — valid token but insufficient permissions
  - `404 Not Found` — resource does not exist
  - `409 Conflict` — entity or registration already exists
- **Pagination**: use `limit` and `offset` query parameters; return `NGSILD-Results-Count` response header if `count`

## NGSI-LD Error Format

All error responses must follow the `ProblemDetails` structure defined in [RFC7807](https://tools.ietf.org/html/rfc7807):

```json
{
  "type": "https://uri.etsi.org/ngsi-ld/errors/InvalidRequest",
  "title": "Invalid request",
  "detail": "The attribute 'temperature' is not defined in the provided context"
}
```

- `type`: a URI from the NGSI-LD error type registry
- `title`: short human-readable summary (stable, not localised)
- `detail`: detailed explanation of this specific occurrence
- Content-Type: `application/json` (error responses do not use `application/ld+json`)

Common NGSI-LD error type URIs:
- `https://uri.etsi.org/ngsi-ld/errors/InvalidRequest`
- `https://uri.etsi.org/ngsi-ld/errors/BadRequestData`
- `https://uri.etsi.org/ngsi-ld/errors/AlreadyExists`
- `https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound`
- `https://uri.etsi.org/ngsi-ld/errors/InternalError`
- `https://uri.etsi.org/ngsi-ld/errors/TooManyResults`
- `https://uri.etsi.org/ngsi-ld/errors/AccessUnauthorized`

All the error types are defined in *shared/src/main/kotlin/com/egm/stellio/shared/model/ApiExceptions.kt*

## Content Negotiation

- Default response format: `application/ld+json` (includes `@context`)
- Clients may request `application/json` (context returned in `Link` header instead)
- For query endpoints, also accept `application/geo+json`

## Distributed / Federated Responses

- When a distributed operation produces partial results (some CSRs unreachable), include an `NGSILD-Warning` response header listing the failed sources
- Never fail the entire response because one CSR is unavailable — return local results plus all successfully retrieved remote results
