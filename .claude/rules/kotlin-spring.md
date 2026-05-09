# Kotlin and Spring Rules

## Formatting

- Indentation: 4 spaces (no tabs)
- Line Length: 120 characters maximum
- Braces: K&R style (opening brace on the same line)
- Imports: Grouped and sorted (Kotlin stdlib first, then Java)
- Detekt: All code must pass Detekt; suppress only with documented rationale and update module baseline

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
