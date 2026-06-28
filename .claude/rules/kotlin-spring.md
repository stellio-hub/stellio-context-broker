# Kotlin and Spring Rules

## Formatting

- Indentation: 4 spaces (no tabs)
- Line Length: 120 characters maximum
- Braces: K&R style (opening brace on the same line)
- Imports: Grouped and sorted (Kotlin stdlib first, then Java)
- Detekt: All code must pass Detekt; suppress only with documented rationale and update module baseline

## Comments

- Default to no comments. Add one only when it captures a non-obvious WHY (a hidden constraint, an
  invariant, a workaround) that a future reader could not derive from the code itself.
- Cap comments at ~5 lines. Do not narrate design deliberation — alternatives considered and rejected,
  benchmark numbers, the history of how a bug was found — in code; that belongs in the PR description
  or an ADR, not inline.
- If the same invariant applies to multiple call sites (e.g., a concurrency-safety pattern reused
  across several SQL statements), explain it once at the primary site and reference it from the others
  with a short pointer (e.g., `// see EntityService.patchEntityPayload`) instead of restating it.
- Never reference a file that isn't committed to the repository (a local plan/notes/scratch doc) — the
  comment must be self-contained for whoever reads it later.
- Comments (including test comments) describe current, intended behavior — not debugging narration
  like "this is what regressed" or "even on the buggy code".

## Naming Conventions

- Packages: `lowercase.with.dots` (e.g., `com.egm.stellio.search`)
- Classes/Interfaces: `PascalCase` (e.g., `ContextSearchService`)
- Functions/Variables: `camelCase` (e.g., `searchEntities()`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `MAX_RESULTS`)
- Test Classes: `ClassNameTests` suffix (e.g., `SearchServiceTests`)

## Code Structure

- Class Order: Properties → Init blocks → Secondary constructors → Methods
- Function Order: Public → Internal → Private
- Visibility: Prefer `private` by default, widen only as needed. Use `internal` when needed for unit tests.
- Method length: Avoid methods longer than 30 lines; extract into private helpers

## Kotlin

- Prefer immutable data classes
- Prefer `val` over `var`
- Use null-safety properly; avoid `!!`
- Use extension functions for mapping and transformation
- Use static utility classes for functions that don't need an instance

## Spring

- Use constructor injection only; do not use field injection
- Use `@Service`, `@RestController` appropriately
- Configuration must be in `@Configuration` classes
- Do not put business logic in controllers
- Controllers are in the `web` package, services in the `service` package, config in the `config` package, models in 
the `model` package, and utility functions in the `util` package

## Reactive

- Prefer Kotlin coroutines (`suspend` functions) when writing new service code

## Error Handling

- Domain exceptions use `APIException` (or its subtypes) with an HTTP status
- Services throw domain exceptions; controllers translate them to HTTP responses via `@ControllerAdvice`
- Repositories wrap persistence exceptions into domain exceptions
- **Never use exceptions for control flow in business or parsing logic.** Represent parse and validation failures as
  `Either.Left` using Arrow `Either` — use the `either { }` builder (`arrow.core.raise.either`) with `raise()` for
  errors and `.bind()` to propagate them.
- Use Arrow `Either` for recoverable errors in functional pipelines; use `IorNel` when partial success with warnings
  is needed
- Extract all error messages in the *shared/src/main/kotlin/com/egm/stellio/shared/util/ErrorMessages.kt* class; do not
  hardcode them.

## Logging

```kotlin
private val logger = LoggerFactory.getLogger(javaClass)
```

- `debug`: detailed tracing, not visible in production by default
- `info`: important runtime events (entity created, subscription triggered)
- `warn`: recoverable issues (CSR unreachable, partial result)
- `error`: unrecoverable errors, always include the exception instance

Include relevant IDs (entity ID, tenant) in log messages. Never log sensitive data (tokens, credentials).
