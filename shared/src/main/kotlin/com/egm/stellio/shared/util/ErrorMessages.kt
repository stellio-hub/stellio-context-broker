package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_IRI
import org.springframework.http.MediaType
import java.net.URI

/**
 * Centralised error message definitions.
 *
 * This file provides standardised error messages that follow the guidelines defined in
 * docs/contributing/error_messages_guideline.md. All messages use:
 * - Present tense and active voice
 * - Sentence case
 * - Consistent terminology
 * - User-friendly language
 */

object ErrorMessages {
    object Authorization {
        fun userNotAuthorizedToCreateEntityMessage(entityId: URI? = null) =
            if (entityId != null) "User is not authorized to create entity $entityId"
            else "User is not authorized to create entity"

        fun userNotAuthorizedToReadEntityMessage(entityId: URI? = null) =
            if (entityId != null) "User is not authorized to read entity $entityId"
            else "User is not authorized to read entity"

        fun userNotAuthorizedToUpdateEntityMessage(entityId: URI? = null) =
            if (entityId != null) "User is not authorized to modify entity $entityId"
            else "User is not authorized to modify entity"

        fun userNotAuthorizedToAdminEntityMessage(entityId: URI? = null) =
            if (entityId != null) "User is not authorized to admin entity $entityId"
            else "User is not authorized to admin entity"

        fun userNotHavingRequiredRolesMessage(roles: Set<GlobalRole>) =
            "User does not have any of the required roles: $roles"

        fun userNotAuthorizedToAccessSubscriptionMessage(subscriptionId: URI) =
            "User is not authorized to access subscription $subscriptionId"

        fun subjectNotFoundMessage(sub: String) = "Subject information for $sub does not exist"
    }

    object BatchOperation {
        const val BATCH_PAYLOAD_EMPTY_MESSAGE = "Batch request payload cannot be empty"
        const val PAYLOAD_SINGLE_INSTANCE_MESSAGE = "Payload must contain a single attribute instance"
        const val PAYLOAD_MUST_BE_PROPERTY_MESSAGE = "Payload must be a property"
        const val CANNOT_DESERIALIZE_BATCH_PAYLOAD_TO_LIST_MESSAGE =
            "Payload cannot be deserialized into a list of object. Make sure the provided payload is a list"
    }

    object Csr {
        fun csrNotFoundMessage(id: URI) = "Context source registration $id does not exist"
        fun csrAlreadyExistsMessage(id: URI) = "Context source registration $id already exists"
        fun csrFailedToParseMessage(cause: String?) = "Context source registration cannot be parsed: $cause"

        const val CSR_REGISTRATION_INFO_EMPTY_MESSAGE = "RegistrationInfo should have at least one element"

        fun csrDoesNotSupportDeletionMessage(csrId: URI) =
            "Context source registration $csrId does not support the deletion of entities"
        fun csrDoesNotSupportCreationMessage(csrId: URI) =
            "Context source registration $csrId does not support the creation of entities"

        const val CONTEXT_SOURCE_BADLY_FORMED_ERROR_MESSAGE = "Context source sent a badly formed error"
        const val CONTEXT_SOURCE_DEFAULT_ERROR_MESSAGE = "Context source provided no additional detail about the error"
        const val CONTEXT_SOURCE_MULTISTATUS_MESSAGE = "Context source returned a 207 Multi-Status response"
        fun contextSourceNoErrorMessage(csrId: URI, uri: URI) =
            "Context source registration $csrId did not provide any error message at URI $uri"
        fun contextSourceContactErrorMessage(csrId: URI, uri: URI) =
            "Context source registration $csrId could not be contacted at URI $uri"
    }

    object DataRepresentation {
        fun invalidCharacterInNameMessage(name: Any?) =
            "JSON-LD object contains a member with invalid characters (4.6.2): $name"
        fun invalidCharacterInContentMessage(content: Any?) =
            "JSON-LD object contains a member with invalid characters in value (4.6.4): $content"
        fun invalidCharacterInScopeMessage(name: Any?) =
            "JSON-LD object contains a scope with invalid characters (4.18): $name"
        const val NULL_VALUE_IN_CONTENT_MESSAGE = "JSON-LD object contains a member with a null value (5.5.4)"
    }

    object DbQuery {
        const val OPERATION_NO_RESULT_MESSAGE = "Operation did not return any result"
    }

    object Entity {
        fun entityNotFoundMessage(entityId: String) = "Entity $entityId does not exist"
        fun entityNotFoundMessage(entityId: URI) = "Entity $entityId does not exist"
        fun entityAlreadyExistsMessage(entityId: URI) = "Entity $entityId already exists"

        const val ENTITY_MISSING_ID_MESSAGE = "Could not extract id from JSON-LD entity"
        const val ENTITY_MISSING_TYPE_MESSAGE = "Could not extract type from JSON-LD entity"
        const val ENTITY_ID_MISMATCH_MESSAGE =
            "The id contained in the body is not the same as the one provided in the URL"
        const val ENTITY_MISSING_TYPE_PROPERTY_MESSAGE = "The provided NGSI-LD entity does not contain a type property"
        const val ENTITY_MISSING_ID_PROPERTY_MESSAGE = "The provided NGSI-LD entity does not contain an id property"

        fun entityOrAttrsNotFoundMessage(entityId: URI, attrs: Set<String>) =
            "Entity $entityId does not exist or has none of the requested attributes: $attrs"
        fun attributeNotFoundMessage(attrId: String, entityId: URI) =
            "Attribute '$attrId' does not exist in entity $entityId"
        fun attributeWithDatasetIdNotFoundMessage(attributeName: String, datasetId: URI? = null) =
            if (datasetId == null)
                "Attribute $attributeName (default datasetId) does not exist"
            else
                "Attribute $attributeName (datasetId: $datasetId) does not exist"
        fun attributeInvalidOrNotImplementedTypeMessage(attributeName: String, attributeType: String? = null) =
            "Attribute $attributeName has an invalid or not implemented type: $attributeType"
        fun attributeInvalidOrNotImplementedTypeMessage(attributeName: String) =
            "Attribute $attributeName has an invalid or not implemented type"

        const val ATTRIBUTE_TYPE_MISMATCH_MESSAGE = "The type of the attribute has to be the same as the existing one"

        fun attributeInconsistentInstanceTypesMessage(name: String) =
            "Attribute $name cannot have instances with different types"
        fun attributeMultipleDefaultInstancesMessage(name: String) =
            "Attribute $name cannot have more than one default instance"
        fun attributeMultipleInstancesSameDatasetIdMessage(name: String) =
            "Attribute $name cannot have more than one instance with the same datasetId"
        fun attributeForbiddenMemberMessage(name: String, member: String) =
            "Attribute $name has an instance with a forbidden member: $member"

        fun attributeCannotGetValueMessage(attribute: String) = "Cannot get value from attribute: $attribute"

        fun propertyMissingValueMessage(name: String) = "Property $name has an instance without a value"
        fun attributeMissingValueMessage(memberValue: String, attributeName: String = "") =
            "Attribute $attributeName has an instance without a $memberValue member"

        fun geoPropertyMissingValueMessage(name: String) = "GeoProperty $name has an instance without a value"
        fun jsonPropertyMissingJsonMessage(name: String) = "JsonProperty $name has an instance without a json member"
        fun jsonPropertyInvalidJsonMessage(name: String) =
            "JsonProperty $name has a json member that is not a JSON object, nor an array of JSON objects"
        fun languagePropertyMissingLanguageMapMessage(name: String) =
            "LanguageProperty $name has an instance without a languageMap member"
        fun languagePropertyInvalidLanguageMapMessage(name: String) =
            "LanguageProperty $name has an invalid languageMap member"
        fun vocabPropertyMissingVocabMessage(name: String) =
            "VocabProperty $name has an instance without a vocab member"
        fun vocabPropertyInvalidVocabMessage(name: String) =
            "VocabProperty $name has a vocab member that is not a string, nor an array of string"

        fun relationshipMissingObjectMessage(name: String) = "Relationship $name does not have an object field"
        fun relationshipEmptyMessage(name: String) = "Relationship $name is empty"
        fun relationshipInvalidObjectTypeMessage(name: String, javaClass: Class<*>) =
            "Relationship $name has an invalid object type: ${javaClass.simpleName}"
        fun relationshipInvalidObjectIdMessage(name: String, objectValue: Any?) =
            "Relationship $name has an invalid or no object id: $objectValue"

        const val CANNOT_ADD_SUBATTRIBUTE_MESSAGE = "Cannot add a sub-attribute into empty or multi-instance attribute"
        const val EXPECTED_SINGLE_ENTRY_MESSAGE = "Expected a single entry but got none or more than one"
        const val NOT_IMPLEMENTED_PARTIAL_ATTRIBUTE_MESSAGE =
            "Partial attribute update currently require the presence of the main subattribute"
    }

    object EntityTypeInfo {
        fun typeNotFoundMessage(type: String) = "Type $type does not exist"
    }

    object GenericValidation {

        fun invalidUriMessage(identifier: String) = "Invalid URI: $identifier"
        fun invalidTypeUriMessage(type: String) = "Type $type should be a valid URI"
        fun invalidTypeMessage(expectedType: String) = "Type must be '$expectedType'"

        fun memberMustBePositiveMessage(memberName: String) = "Member '$memberName' must be greater than zero"
        fun memberMustBeFutureMessage(memberName: String) = "Member '$memberName' must be in the future"
        fun memberIsInvalidMessage(memberName: String) = "Member '$memberName' is invalid"

        fun atLeastOneRequiredMessage(vararg members: String) =
            "At least one of ${members.joinToString(" or ") { "'$it'" }} is required"
        fun cannotSpecifyBothMessage(member1: String, member2: String) =
            "Cannot specify both '$member1' and '$member2'"
        fun cannotSpecifyWhenPresent(member: String, vararg members: String) =
            "Cannot specify '$member' when ${members.joinToString(" or ") { "'$it'" }} are present"

        fun invalidGeometryDefinitionMessage(geoJsonPayload: String, error: String) =
            "Invalid geometry definition: $geoJsonPayload ($error)"
    }

    object HttpRequest {
        fun unsupportedAcceptHeaderMessage(acceptHeaders: List<MediaType>) =
            "Unsupported Accept header value: ${acceptHeaders.joinToString(",")}"
        const val UNSUPPORTED_MEDIA_TYPE_MESSAGE = "Unsupported media type"
        const val NOT_ACCEPTABLE_MESSAGE = "Not acceptable"

        fun badlyFormedLinkHeaderMessage(linkHeader: String) = "Link header is badly formed: $linkHeader"
        const val MISSING_AT_CONTEXT_JSON_LD_CONTENT_TYPE_MESSAGE =
            "Request payload must contain @context term for a request having an application/ld+json content type"
        const val INVALID_AT_CONTEXT_JSON_CONTENT_TYPE_MESSAGE =
            "Request payload must not contain @context term for a request having an application/json content type"
        const val INVALID_CONTEXT_LINK_JSON_LD_CONTENT_TYPE_MESSAGE =
            "JSON-LD Link header must not be provided for a request having an application/ld+json content type"
    }

    object Json {
        const val JSON_PARSING_ERROR_MESSAGE = "JSON payload could not be parsed"

        fun cannotDeserializeToObjectMessage(cause: String? = null) =
            if (cause != null) "Data cannot be deserialized into an object ($cause)"
            else "Data cannot be deserialized into an object"
        fun cannotDeserializeToListMessage(cause: String? = null) =
            if (cause != null) "Data cannot be deserialized into list of objects ($cause)"
            else "Data cannot be deserialized into list of objects"
    }

    object JsonLdContextServer {
        fun contextNotFoundInCacheMessage(context: String) = "Context with id $context was not found in the cache"
        fun contextInvalidMessage(context: String, cause: String? = null) =
            if (cause != null) "Provided context is invalid: $context ($cause)"
            else "Provided context is invalid: $context"
    }

    object JsonLd {
        const val UNABLE_TO_LOAD_REMOTE_CONTEXT_MESSAGE = "Remote context could not be loaded"
        const val UNEXPECTED_ERROR_PARSING_PAYLOAD_MESSAGE = "JSON-LD payload could not be parsed"
        const val UNABLE_TO_EXPAND_PAYLOAD_MESSAGE = "JSON-LD payload could not be expanded"
    }

    object Permission {
        fun permissionNotFoundMessage(id: URI) = "Permission $id does not exist"
        fun permissionAlreadyExistsMessage(id: URI) = "Permission $id already exists"
        fun permissionAlreadyCoveredMessage(id: URI) = "Permission $id already covers the created permission"
        fun permissionFailedToParseMessage(cause: String?) = "Permission cannot be parsed: $cause"

        fun unauthorizedTargetMessage(target: String) = "User is not authorized to administer target: $target"
        fun unauthorizedRetrieveMessage(permissionId: URI) = "User is not authorized to read permission $permissionId"

        const val GLOBAL_POLICY_RESTRICTION_MESSAGE = "Only read and write are accepted as global policy"
        const val OWN_PERMISSION_CREATE_UPDATE_PROHIBITED_MESSAGE =
            "Permission with an 'own' action cannot be updated or created"
        const val OWN_PERMISSION_DELETE_PROHIBITED_MESSAGE = "Permission with an 'own' action cannot be deleted"
        const val AUTHENTICATED_ADMIN_PROHIBITED_MESSAGE = "Admin right cannot be added for every authenticated user"
        const val PUBLIC_NON_READ_PROHIBITED_MESSAGE = "Non-read right cannot be added for public access"

        fun invalidActionMessage(action: String) =
            "Invalid action: '$action', must be 'own', 'admin', 'write', or 'read'"
    }

    object QueryParameter {
        fun unparsableQueryMessage(message: String?) = "Query could not be parsed: $message"

        const val MISSING_REQUIRED_QUERY_PARAMETER_MESSAGE =
            "One of 'type', 'attrs', 'q', 'geoQ' must be provided in the query unless local is true"

        fun invalidJoinParameterMessage(param: String) =
            "Invalid 'join' parameter: '$param', must be 'flat', 'inline', or '@none'"
        fun invalidJoinLevelParameterMessage(param: String) =
            "Invalid 'joinLevel' parameter: '$param', must be a positive integer"
        const val JOIN_REQUIRED_WITH_JOIN_LEVEL_OR_CONTAINED_BY_MESSAGE =
            "Parameter 'join' is required when 'joinLevel' or 'containedBy' are specified"

        const val OFFSET_AND_LIMIT_MUST_BE_POSITIVE_MESSAGE = "Offset and limit must be greater than zero"
        const val OFFSET_AND_LIMIT_MUST_BE_POSITIVE_NO_COUNT_MESSAGE =
            "Offset must be greater than zero and limit must be strictly greater than zero"
        fun tooHighLimitMessage(limit: Int, maxLimit: Int) =
            "You asked for $limit results, but the supported maximum limit is $maxLimit"

        fun invalidIdPatternMessage(idPattern: String?) = "Invalid pattern in parameter 'idPattern': $idPattern"
        const val INVALID_ID_PATTERN_MESSAGE = "Invalid pattern in member 'idPattern'"

        fun invalidNearGeorelExpressionMessage(georel: String) = "Invalid expression for 'near' georel: $georel"
        fun invalidGeorelParameterMessage(georel: String) = "Invalid 'georel' parameter provided: $georel"
        fun unrecognizedGeometryValueMessage(geometry: String) =
            "$geometry is not a recognized value for 'geometry' parameter"
        const val INVALID_GEOQUERY_MESSAGE =
            "Missing at least one geo parameter between 'geometry', 'georel' and 'coordinates'"

        fun invalidFormatValueMessage(format: String) = "'$format' is not a valid value for the format query parameter"
        fun invalidOptionsValueMessage(option: String) =
            "'$option' is not a valid value for the options query parameter"
        fun invalidOrderingDirectionMessage(direction: String) =
            "'$direction' is not a valid ordering direction parameter"

        const val ENTITY_CORE_MEMBERS_IN_ATTRS_MESSAGE = "Entity core members cannot be present in 'attrs' parameter"

        const val ATTRIBUTES_WITH_PICK_OR_OMIT_MESSAGE = "Cannot use 'attrs' with 'pick' or 'omit'"
        const val NO_ENTITY_MEMBER_AFTER_PROJECTION_MESSAGE = "No entity member left after applying pick and omit"
        const val ENTITY_MEMBER_IN_PICK_AND_OMIT_MESSAGE = "Entity member cannot be present in both 'pick' and 'omit'"
        const val PARSING_PICK_OMIT_ERROR_MESSAGE = "Error parsing pick or omit parameter"
        const val PROJECTION_VALUE_EMPTY_MESSAGE = "Value cannot be empty"
        const val PROJECTION_STARTS_WITH_BRACE_MESSAGE = "Expression cannot start with a brace, comma or pipe"
        const val PROJECTION_ENDS_WITH_SEPARATOR_MESSAGE = "Expression cannot end with a separator"
        const val PROJECTION_EMPTY_ATTRIBUTE_NAME_MESSAGE = "Expression contains an empty attribute name"
        const val PROJECTION_NO_VALID_ATTRIBUTE_MESSAGE = "Expression must contain at least one valid attribute name"
        const val PROJECTION_SEPARATOR_AFTER_BRACE_MESSAGE =
            "Expression cannot contain a separator after an opening brace"
        const val PROJECTION_EMPTY_ATTRIBUTE_NESTED_MESSAGE = "Expression contains an empty nested attribute"
        const val PROJECTION_UNCLOSED_BRACE_MESSAGE = "Expression contains an unclosed brace"
        const val PROJECTION_EMPTY_NESTED_MESSAGE = "Expression contains an empty nested projection"
        const val PROJECTION_CONSECUTIVE_SEPARATORS_MESSAGE = "Expression cannot contain consecutive separators"
        fun projectionInvalidCharactersMessage(invalidChars: String) = "Invalid characters in the value ($invalidChars)"
    }

    object Scope {
        const val SCOPE_DOES_NOT_EXIST_MESSAGE = "Scope does not exist and operation does not allow creating it"
        fun unrecognizedOperationTypeMessage(operationType: String) = "Unrecognized operation type: $operationType"
    }

    object Subscription {
        fun subscriptionNotFoundMessage(subscriptionId: URI) = "Subscription $subscriptionId does not exist"
        fun subscriptionAlreadyExistsMessage(subscriptionId: URI) = "Subscription $subscriptionId already exists"
        fun subscriptionFailedToParseMessage(cause: String?) = "Subscription cannot be parsed: $cause"

        fun notImplementedMemberMessage(memberName: String) = "Member '$memberName' is not yet implemented"
        fun unknownNotificationTriggerMessage(trigger: String) = "Unknown notification trigger: $trigger"
        fun invalidEndpointUriMessage(uri: String) = "Invalid URI for endpoint: $uri"

        const val SHOW_CHANGES_WITH_SIMPLIFIED_MESSAGE =
            "Cannot use 'showChanges' with 'simplified' or 'keyValues' format"
    }

    object Temporal {
        const val INVALID_TEMPORAL_INSTANCE_MESSAGE =
            "One attribute instance is missing the required $NGSILD_OBSERVED_AT_IRI temporal property"
        const val INCONSISTENT_VALUES_IN_AGGREGATION_MESSAGE =
            "Aggregation method tried to aggregate inconsistent types of values"
        const val DIFFERENT_TEMPORAL_REPRESENTATIONS_MESSAGE =
            "Found different temporal representations in 'options' query parameter, only one can be provided"
        const val END_TIME_AT_MANDATORY_FOR_BETWEEN_MESSAGE =
            "'endTimeAt' request parameter is mandatory if 'timerel' is 'between'"
        const val AGGR_METHODS_MANDATORY_MESSAGE =
            "'aggrMethods' is mandatory if 'aggregatedValues' option is specified"

        fun attributeOrInstanceNotFoundMessage(attributeName: String, instanceId: String) =
            "Instance $instanceId does not exist or attribute $attributeName does not exist"

        fun unrecognizedAggregationMethodMessage(method: String) =
            "'$method' is not a recognized aggregation method for 'aggrMethods' parameter"

        fun invalidTimePropertyMessage(timeProperty: String) = "Invalid value for 'timeproperty': $timeProperty"
        fun invalidTemporalRepresentationMessage(representation: String) =
            "Invalid value for temporal representation: '$representation'"
    }

    object Tenant {
        fun tenantNotFoundMessage(tenant: String) = "Tenant $tenant does not exist"
        fun issuerPropertyName(index: Int) = "application.tenants.$index.issuer"
        const val ISSUER_MANDATORY_WHEN_AUTHENTICATION_ENABLED =
            "Issuer is mandatory when authentication is enabled"
    }
}
