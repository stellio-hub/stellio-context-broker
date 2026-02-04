package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_IRI
import java.net.URI

/**
 * Centralized error message definitions for the Stellio Context Broker.
 *
 * This file provides standardised error messages that follow the guidelines defined in
 * docs/error-messages-guideline.md. All messages use:
 * - Present tense and active voice
 * - Sentence case
 * - Consistent terminology
 * - User-friendly language
 *
 * Messages are organized by category:
 * 1. Resource Lifecycle (not found, already exists)
 * 2. Validation Errors (invalid values, required fields)
 * 3. Authorization Errors
 * 4. JSON-LD and Data Errors
 * 5. Temporal and Aggregation Errors
 * 6. Batch Operation Errors
 */

object EntityErrorMessages {
    fun entityNotFoundMessage(entityId: String) = "Entity $entityId does not exist"
    fun entityNotFoundMessage(entityId: URI) = "Entity $entityId does not exist"
    fun entityAlreadyExistsMessage(entityId: String) = "Entity $entityId already exists"
    fun entityAlreadyExistsMessage(entityId: URI) = "Entity $entityId already exists"

    const val ENTITY_DOES_NOT_EXIST_MESSAGE = "Entity does not exist"
    const val ENTITY_ALREADY_EXISTS_MESSAGE = "Entity already exists"

    fun entityOrAttrsNotFoundMessage(
        entityId: URI,
        attrs: Set<String>
    ) = "Entity $entityId does not exist or has none of the requested attributes: $attrs"

    fun typeNotFoundMessage(type: String) = "Type $type does not exist"

    fun attributeNotFoundMessage(attributeName: String, datasetId: URI? = null) =
        if (datasetId == null)
            "Attribute $attributeName (default datasetId) does not exist"
        else
            "Attribute $attributeName (datasetId: $datasetId) does not exist"

    fun attributeOrInstanceNotFoundMessage(
        attributeName: String,
        instanceId: String
    ) = "Instance $instanceId does not exist or attribute $attributeName does not exist"
}

object PermissionErrorMessages {
    fun permissionNotFoundMessage(id: URI) = "Permission $id does not exist"
    fun permissionAlreadyExistsMessage(id: URI) = "Permission $id already exists"
    fun permissionAlreadyCoveredMessage(id: URI) = "Permission $id already covers the created permission"

    const val GLOBAL_POLICY_RESTRICTION_MESSAGE = "Only read and write are accepted as global policy"
    const val OWN_PERMISSION_CREATE_UPDATE_PROHIBITED_MESSAGE = "Creating or updating an 'own' permission is prohibited"
    const val OWN_PERMISSION_DELETE_PROHIBITED_MESSAGE = "Deleting an 'own' permission is prohibited"
    const val AUTHENTICATED_ADMIN_PROHIBITED_MESSAGE = "Adding admin right for every authenticated user is prohibited"
    const val PUBLIC_NON_READ_PROHIBITED_MESSAGE = "Adding non-read right for public access is prohibited"
}

object CsrErrorMessages {
    fun csrNotFoundMessage(id: URI) = "Context source registration $id does not exist"
    fun csrAlreadyExistsMessage(id: URI) = "Context source registration $id already exists"
}

object QueryErrorMessages {
    const val OPERATION_NO_RESULT_MESSAGE = "Operation did not return any result"
}

object ValidationErrorMessages {

    fun invalidUriMessage(identifier: String) = "Invalid URI: $identifier"
    fun invalidTypeMessage(expectedType: String) = "Type must be '$expectedType'"

    fun fieldMustBePositiveMessage(fieldName: String) = "Field '$fieldName' must be greater than zero"
    fun fieldMustBeFutureMessage(fieldName: String) = "Field '$fieldName' must be in the future"
    fun fieldIsInvalidMessage(fieldName: String) = "Field '$fieldName' is invalid"

    fun atLeastOneRequiredMessage(vararg fields: String) =
        "At least one of ${fields.joinToString(" or ") { "'$it'" }} is required"
    fun exactlyOneRequiredMessage(vararg fields: String) =
        "Exactly one of ${fields.joinToString(" or ") { "'$it'" }} is required"
    fun cannotSpecifyBothMessage(field1: String, field2: String) =
        "Cannot specify both '$field1' and '$field2'"

    fun invalidPatternMessage(fieldName: String) = "Invalid pattern in field '$fieldName'"
}

object BatchOperationErrorMessages {
    const val BATCH_PAYLOAD_EMPTY_MESSAGE = "Batch request payload cannot be empty"
    const val PAYLOAD_SINGLE_INSTANCE_MESSAGE = "Payload must contain a single attribute instance"
    const val PAYLOAD_MUST_BE_PROPERTY_MESSAGE = "Payload must be a property"
}

object AuthorizationErrorMessages {
    fun userNotAuthorizedToCreateEntityMessage(entityId: URI? = null) =
        if (entityId != null) "User is not authorized to create entity $entityId"
        else "User is not authorized to create entity"

    fun userNotAuthorizedToReadEntityMessage(entityId: URI? = null) =
        if (entityId != null) "User is not authorized to read entity $entityId"
        else "User is not authorized to read entity"

    fun userNotAuthorizedToUpdateEntityMessage(entityId: URI? = null) =
        if (entityId != null) "User is not authorized to modify entity $entityId"
        else "User is not authorized to modify entity"

    fun userNotAuthorizedToDeleteEntityMessage(entityId: URI? = null) =
        if (entityId != null) "User is not authorized to delete entity $entityId"
        else "User is not authorized to delete entity"

    fun userNotAuthorizedToAdminEntityMessage(entityId: URI? = null) =
        if (entityId != null) "User is not authorized to admin entity $entityId"
        else "User is not authorized to admin entity"

    fun userNotAuthorizedToAccessSubscriptionMessage(subscriptionId: URI) =
        "User is not authorized to access subscription $subscriptionId"

    fun userNotAuthorizedToReadPermissionMessage(permissionId: URI) =
        "User is not authorized to read permission $permissionId"

    fun userNotAuthorizedToAdminTargetMessage(target: Any) =
        "User is not authorized to admin target: $target"
}

object NgsiLdErrorMessages {
    fun invalidCharacterInNameMessage(name: Any?) =
        "JSON-LD object contains a member with invalid characters (4.6.2): $name"

    fun invalidCharacterInContentMessage(content: Any?) =
        "JSON-LD object contains a member with invalid characters in value (4.6.4): $content"

    fun invalidCharacterInScopeMessage(name: Any?) =
        "JSON-LD object contains a scope with invalid characters (4.18): $name"

    const val NULL_VALUE_IN_CONTENT_MESSAGE = "JSON-LD object contains a member with a null value (5.5.4)"

    fun relationshipMissingObjectMessage(name: String) = "Relationship $name does not have an object field"
    fun relationshipEmptyMessage(name: String) = "Relationship $name is empty"
    fun relationshipInvalidObjectTypeMessage(name: String, javaClass: Class<*>) =
        "Relationship $name has an invalid object type: ${javaClass.simpleName}"
    fun relationshipInvalidObjectIdMessage(name: String, objectValue: Any?) =
        "Relationship $name has an invalid or no object id: $objectValue"

    const val CANNOT_ADD_SUBATTRIBUTE_MESSAGE = "Cannot add a sub-attribute into empty or multi-instance attribute"
    const val EXPECTED_SINGLE_ENTRY_MESSAGE = "Expected a single entry but got none or more than one"

    const val SCOPE_DOES_NOT_EXIST_MESSAGE = "Scope does not exist and operation does not allow creating it"
    fun unrecognizedOperationTypeMessage(operationType: String) = "Unrecognized operation type: $operationType"
}

object TemporalErrorMessages {
    const val INVALID_TEMPORAL_INSTANCE_MESSAGE =
        "One attribute instance is missing the required $NGSILD_OBSERVED_AT_IRI temporal property"
    const val INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE =
        "Aggregation method tried to aggregate inconsistent types of values"
}

object SubscriptionErrorMessages {
    fun subscriptionNotFoundMessage(subscriptionId: URI) = "Subscription $subscriptionId does not exist"
    fun subscriptionAlreadyExistsMessage(subscriptionId: URI) = "Subscription $subscriptionId already exists"

    fun unknownNotificationTriggerMessage(trigger: String) = "Unknown notification trigger: $trigger"
    fun invalidEndpointUriMessage(uri: String) = "Invalid URI for endpoint: $uri"
}

// =============================================================================
// Query Parameter Errors
// =============================================================================

object QueryParameterErrorMessages {
    fun invalidJoinParameterMessage(param: String) = "Invalid join parameter: $param"
    fun invalidJoinLevelParameterMessage(param: String) = "Invalid join level parameter: $param"
}
