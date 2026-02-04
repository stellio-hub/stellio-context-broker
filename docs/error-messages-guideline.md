# Error Messages Guidelines

## Purpose

This document establishes conventions for error messages in the Stellio Context Broker to ensure consistency, clarity, and a better user experience.

## Core Principles

1. **User-Focused**: Write for API consumers, not developers
2. **Descriptive**: State what is wrong, not what the system tried to do
3. **Actionable**: When possible, hint at how to fix the issue
4. **Consistent**: Follow the same patterns across the codebase
5. **Professional**: Clear and respectful tone without being overly formal

## Wording Guidelines

### 1. Voice and Tense

- **Use present tense** for describing the current state
- **Use active voice** instead of passive voice
- **Be direct** without unnecessary words

✅ **Good**: `Entity urn:ngsi-ld:Device:001 does not exist`
❌ **Avoid**: `Entity urn:ngsi-ld:Device:001 was not found`
❌ **Avoid**: `The entity could not be found`

### 2. Capitalization

- **Use sentence case** (capitalize only the first word and proper nouns)
- **Do not use title case or all caps** (except for acronyms like URI, JSON-LD)

✅ **Good**: `Attribute temperature does not exist`
❌ **Avoid**: `Attribute Temperature Does Not Exist`
❌ **Avoid**: `ATTRIBUTE TEMPERATURE DOES NOT EXIST`

### 3. Structure

Follow a consistent structure: **Subject + Verb + Context**

```
<Resource/Field> <verb> <additional context>
```

**Examples:**
- `Entity {id} does not exist`
- `Attribute {name} is invalid`
- `Field 'expiresAt' must be in the future`
- `Type must be 'Subscription'`

### 4. Terminology

Use consistent terminology across all messages:

| **Concept** | **Use** | **Avoid** |
|-------------|---------|-----------|
| Non-existent resources | `does not exist` | `not found`, `could not find`, `missing` |
| Already existing resources | `already exists` | `duplicate`, `exists already` |
| Invalid values | `is invalid` | `is not valid`, `not correct` |
| Required fields | `is required` | `must be present`, `shall be present` |
| Type mismatches | `must be {type}` | `should be {type}`, `expected {type}` |
| Value constraints | `must be {constraint}` | `should be {constraint}`, `needs to be {constraint}` |

### 5. Avoid Technical Jargon

Replace internal/technical terms with user-friendly alternatives:

✅ **Good**: `Type must be 'Subscription'`
❌ **Avoid**: `type attribute must be equal to 'Subscription'`

✅ **Good**: `Subscription {id} already exists`
❌ **Avoid**: `A subscription with id {id} already exists`

✅ **Good**: `Field 'timeInterval' must be greater than zero`
❌ **Avoid**: `The value of 'timeInterval' must be greater than zero (int)`

### 6. Be Specific

Include relevant identifiers and context to help users diagnose the issue:

✅ **Good**: `Entity urn:ngsi-ld:Device:001 does not exist`
❌ **Avoid**: `Entity does not exist`

✅ **Good**: `Attribute temperature (datasetId: urn:ngsi-ld:Dataset:01) does not exist`
❌ **Avoid**: `Attribute does not exist`

### 7. Multiple Requirements

For validation with multiple alternatives:

✅ **Good**: `At least one of 'entities' or 'watchedAttributes' is required`
❌ **Avoid**: `At least one of entities or watchedAttributes shall be present`

✅ **Good**: `Field 'target' must specify either 'id', 'types', or 'scopes'`
❌ **Avoid**: `You must specify a target id, types or scopes`

### 8. Avoid Contractions and Informal Language

- Use full forms: `cannot` instead of `can't`, `does not` instead of `doesn't`
- Avoid informal pronouns like "you"

✅ **Good**: `Field 'target' must specify either 'id', 'types', or 'scopes'`
❌ **Avoid**: `You must specify a target id, types or scopes`

✅ **Good**: `Cannot specify both 'id' and 'types' in target`
❌ **Avoid**: `You can't target an id and types or scopes`

### 9. Authorization and Permission Errors

Use clear, consistent phrasing:

✅ **Good**: `User is not authorized to access subscription {id}`
✅ **Good**: `User is not authorized to modify entity {id}`
❌ **Avoid**: `User forbidden to modify entity`
❌ **Avoid**: `Access denied to entity {id}`

## Exception Type Guidelines

### BadRequestDataException
For invalid or malformed request data:
- Invalid field values
- Type mismatches
- Constraint violations
- Missing required fields

**Examples:**
```kotlin
BadRequestDataException("Type must be 'Subscription'")
BadRequestDataException("Field 'timeInterval' must be greater than zero")
BadRequestDataException("At least one of 'entities' or 'watchedAttributes' is required")
```

### InvalidRequestException
For invalid request structure or query parameters:
- Malformed query parameters
- Invalid JSON-LD structure
- Invalid combinations of parameters

**Examples:**
```kotlin
InvalidRequestException("Query parameter 'georel' is invalid")
InvalidRequestException("Batch request payload cannot be empty")
```

### ResourceNotFoundException
For non-existent resources:

**Examples:**
```kotlin
ResourceNotFoundException("Entity $entityId does not exist")
ResourceNotFoundException("Subscription $subscriptionId does not exist")
ResourceNotFoundException("Attribute $attributeName does not exist")
```

### AlreadyExistsException
For duplicate resource creation:

**Examples:**
```kotlin
AlreadyExistsException("Entity $entityId already exists")
AlreadyExistsException("Subscription $subscriptionId already exists")
```

### AccessDeniedException
For authorization failures:

**Examples:**
```kotlin
AccessDeniedException("User is not authorized to access subscription $subscriptionId")
AccessDeniedException("User is not authorized to modify entity $entityId")
AccessDeniedException("User is not authorized to delete permission $permissionId")
```

### OperationNotSupportedException
For unsupported operations:

**Examples:**
```kotlin
OperationNotSupportedException("Operation '$operation' is not supported")
OperationNotSupportedException("Aggregation method '$method' is not supported for this attribute type")
```

## Centralization Strategy

### Location of Message Definitions

1. **Common messages** → `shared/src/main/kotlin/com/egm/stellio/shared/util/ErrorMessages.kt`
2. **Domain-specific messages** → Companion objects or local utility files (e.g., `Permission.notFoundMessage()`)
3. **Never hardcode** exception messages directly in business logic

### Message Definition Patterns

**For static messages:**
```kotlin
const val BATCH_PAYLOAD_EMPTY_MESSAGE = "Batch request payload cannot be empty"
```

**For dynamic messages with parameters:**
```kotlin
fun entityNotFoundMessage(entityId: URI) = "Entity $entityId does not exist"
fun entityAlreadyExistsMessage(entityId: URI) = "Entity $entityId already exists"
```

**For complex domain-specific messages:**
```kotlin
// In companion object
companion object {
    fun notFoundMessage(id: URI) = "Subscription $id does not exist"
    fun alreadyExistsMessage(id: URI) = "Subscription $id already exists"
    fun unauthorizedMessage(id: URI) = "User is not authorized to access subscription $id"
}
```

## Examples

### Before and After

#### Example 1: Validation Error
**Before:**
```kotlin
BadRequestDataException("type attribute must be equal to 'Subscription'")
```

**After:**
```kotlin
BadRequestDataException("Type must be 'Subscription'")
```

#### Example 2: Value Constraint
**Before:**
```kotlin
BadRequestDataException("The value of 'timeInterval' must be greater than zero (int)")
```

**After:**
```kotlin
BadRequestDataException("Field 'timeInterval' must be greater than zero")
```

#### Example 3: Resource Not Found
**Before:**
```kotlin
ResourceNotFoundException("Could not find a subscription with id $subscriptionId")
```

**After:**
```kotlin
ResourceNotFoundException("Subscription $subscriptionId does not exist")
```

#### Example 4: Authorization Error
**Before:**
```kotlin
AccessDeniedException("User forbidden to modify entity")
```

**After:**
```kotlin
AccessDeniedException("User is not authorized to modify entity $entityId")
```

#### Example 5: Invalid Combination
**Before:**
```kotlin
BadRequestDataException("You can't target an id and types or scopes")
```

**After:**
```kotlin
BadRequestDataException("Cannot specify both 'id' and 'types' in target")
```

#### Example 6: Missing Requirements
**Before:**
```kotlin
BadRequestDataException("At least one of entities or watchedAttributes shall be present")
```

**After:**
```kotlin
BadRequestDataException("At least one of 'entities' or 'watchedAttributes' is required")
```

## Code Reviewer Checklist

When reviewing code that introduces or modifies error messages, verify:

### ✅ Wording
- [ ] Uses present tense and active voice
- [ ] Uses sentence case (not title case or all caps)
- [ ] Follows the "Subject + Verb + Context" structure
- [ ] Uses consistent terminology (e.g., "does not exist" not "not found")
- [ ] Avoids technical jargon and internal terms
- [ ] Avoids informal language (no "you", "can't", etc.)

### ✅ Specificity
- [ ] Includes relevant identifiers (entity ID, attribute name, etc.)
- [ ] Provides enough context for debugging
- [ ] Is specific enough to be actionable

### ✅ Consistency
- [ ] Matches existing patterns for similar error types
- [ ] Uses the same phrasing as comparable messages
- [ ] Uses the appropriate exception type for the error category

### ✅ Centralization
- [ ] Message is not hardcoded in business logic
- [ ] Common messages are defined in `ErrorMessages.kt`
- [ ] Domain-specific messages are in companion objects or local utilities
- [ ] No duplicate message definitions exist

### ✅ Correctness
- [ ] Message accurately describes the error condition
- [ ] Exception type matches the error category (BadRequestDataException, ResourceNotFoundException, etc.)
- [ ] Message is grammatically correct

### 🔍 Red Flags
- Hardcoded exception messages in service/handler methods
- Inconsistent capitalization or punctuation
- Overly technical or vague error messages
- Use of "could not", "was not", "you", "shall", etc.
- Missing identifiers in error messages
- Duplicate error message definitions

## Migration Path

When refactoring existing error messages:

1. **Identify** hardcoded exception messages in the code
2. **Standardize** the message according to these guidelines
3. **Centralize** the message definition (if reusable)
4. **Replace** all occurrences with the centralized version
5. **Test** that error responses still work as expected
6. **Document** any domain-specific conventions

## Additional Resources

- NGSI-LD Specification Error Types: [ETSI GS CIM 009](https://www.etsi.org/deliver/etsi_gs/CIM/001_099/009/01.07.01_60/gs_CIM009v010701p.pdf)
- Project Exception Types: `shared/src/main/kotlin/com/egm/stellio/shared/model/ApiExceptions.kt`
- Existing Common Messages: `shared/src/main/kotlin/com/egm/stellio/shared/util/ApiResponses.kt`
