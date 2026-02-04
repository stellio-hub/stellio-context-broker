# Error Messages Guidelines

## Purpose

This document establishes conventions for error messages to ensure consistency, clarity, and a better user experience.

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

| **Concept**                | **Use**                | **Avoid**                                            |
|----------------------------|------------------------|------------------------------------------------------|
| Non-existent resources     | `does not exist`       | `not found`, `could not find`, `missing`             |
| Already existing resources | `already exists`       | `duplicate`, `exists already`                        |
| Invalid values             | `is invalid`           | `is not valid`, `not correct`                        |
| Required fields            | `is required`          | `must be present`, `shall be present`                |
| Type mismatches            | `must be {type}`       | `should be {type}`, `expected {type}`                |
| Value constraints          | `must be {constraint}` | `should be {constraint}`, `needs to be {constraint}` |

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
- [ ] Message is defined in `ErrorMessages.kt`
- [ ] No duplicate message definitions exist

### ✅ Correctness
- [ ] Message accurately describes the error condition
- [ ] Exception type matches the error category (BadRequestDataException, ResourceNotFoundException, etc.)
- [ ] Message is grammatically correct
