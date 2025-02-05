package com.egm.stellio.search.csr.model

import com.fasterxml.jackson.annotation.JsonProperty

enum class Operation(val key: String) {
    @JsonProperty("createEntity")
    CREATE_ENTITY("createEntity"),

    @JsonProperty("updateEntity")
    UPDATE_ENTITY("updateEntity"),

    @JsonProperty("appendAttrs")
    APPEND_ATTRS("appendAttrs"),

    @JsonProperty("updateAttrs")
    UPDATE_ATTRS("updateAttrs"),

    @JsonProperty("deleteAttrs")
    DELETE_ATTRS("deleteAttrs"),

    @JsonProperty("deleteEntity")
    DELETE_ENTITY("deleteEntity"),

    @JsonProperty("createBatch")
    CREATE_BATCH("createBatch"),

    @JsonProperty("upsertBatch")
    UPSERT_BATCH("upsertBatch"),

    @JsonProperty("updateBatch")
    UPDATE_BATCH("updateBatch"),

    @JsonProperty("deleteBatch")
    DELETE_BATCH("deleteBatch"),

    @JsonProperty("upsertTemporal")
    UPSERT_TEMPORAL("upsertTemporal"),

    @JsonProperty("appendAttrsTemporal")
    APPEND_ATTRS_TEMPORAL("appendAttrsTemporal"),

    @JsonProperty("deleteAttrsTemporal")
    DELETE_ATTRS_TEMPORAL("deleteAttrsTemporal"),

    @JsonProperty("updateAttrInstanceTemporal")
    UPDATE_ATTRINSTANCE_TEMPORAL("updateAttrInstanceTemporal"),

    @JsonProperty("deleteAttrInstanceTemporal")
    DELETE_ATTRINSTANCE_TEMPORAL("deleteAttrInstanceTemporal"),

    @JsonProperty("deleteTemporal")
    DELETE_TEMPORAL("deleteTemporal"),

    @JsonProperty("mergeEntity")
    MERGE_ENTITY("mergeEntity"),

    @JsonProperty("replaceEntity")
    REPLACE_ENTITY("replaceEntity"),

    @JsonProperty("replaceAttrs")
    REPLACE_ATTRS("replaceAttrs"),

    @JsonProperty("mergeBatch")
    MERGE_BATCH("mergeBatch"),

    // CONSUMPTION
    @JsonProperty("retrieveEntity")
    RETRIEVE_ENTITY("retrieveEntity"),

    @JsonProperty("queryEntity")
    QUERY_ENTITY("queryEntity"),

    @JsonProperty("queryBatch")
    QUERY_BATCH("queryBatch"),

    @JsonProperty("retrieveTemporal")
    RETRIEVE_TEMPORAL("retrieveTemporal"),

    @JsonProperty("queryTemporal")
    QUERY_TEMPORAL("queryTemporal"),

    @JsonProperty("retrieveEntityTypes")
    RETRIEVE_ENTITY_TYPES("retrieveEntityTypes"),

    @JsonProperty("retrieveEntityTypeDetails")
    RETRIEVE_ENTITY_TYPE_DETAILS("retrieveEntityTypeDetails"),

    @JsonProperty("retrieveEntityTypeInfo")
    RETRIEVE_ENTITY_TYPE_INFO("retrieveEntityTypeInfo"),

    @JsonProperty("retrieveAttrTypes")
    RETRIEVE_ATTR_TYPES("retrieveAttrTypes"),

    @JsonProperty("retrieveAttrTypeDetails")
    RETRIEVE_ATTR_TYPE_DETAILS("retrieveAttrTypeDetails"),

    @JsonProperty("retrieveAttrTypeInfo")
    RETRIEVE_ATTR_TYPE_INFO("retrieveAttrTypeInfo"),

    // SUBSCRIPTION
    @JsonProperty("createSubscription")
    CREATE_SUBSCRIPTION("createSubscription"),

    @JsonProperty("updateSubscription")
    UPDATE_SUBSCRIPTION("updateSubscription"),

    @JsonProperty("retrieveSubscription")
    RETRIEVE_SUBSCRIPTION("retrieveSubscription"),

    @JsonProperty("querySubscription")
    QUERY_SUBSCRIPTION("querySubscription"),

    @JsonProperty("deleteSubscription")
    DELETE_SUBSCRIPTION("deleteSubscription"),

    // OPERATION GROUPS
    @JsonProperty("federationOps")
    FEDERATION_OPS("federationOps"),

    @JsonProperty("updateOps")
    UPDATE_OPS("updateOps"),

    @JsonProperty("retrieveOps")
    RETRIEVE_OPS("retrieveOps"),

    @JsonProperty("redirectionOps")
    REDIRECTION_OPS("redirectionOps");

    companion object {

        val matchingOperations = mapOf(
            CREATE_ENTITY to setOf(CREATE_ENTITY, UPDATE_OPS, REDIRECTION_OPS),
            UPDATE_ENTITY to setOf(UPDATE_ENTITY, UPDATE_OPS, REDIRECTION_OPS),
            APPEND_ATTRS to setOf(APPEND_ATTRS, REDIRECTION_OPS),
            UPDATE_ATTRS to setOf(UPDATE_ATTRS, UPDATE_OPS, REDIRECTION_OPS),
            DELETE_ATTRS to setOf(DELETE_ATTRS, REDIRECTION_OPS),
            DELETE_ENTITY to setOf(DELETE_ENTITY, REDIRECTION_OPS),
            CREATE_BATCH to setOf(CREATE_BATCH),
            UPSERT_BATCH to setOf(UPSERT_BATCH),
            UPDATE_BATCH to setOf(UPDATE_BATCH),
            MERGE_BATCH to setOf(MERGE_BATCH),
            DELETE_BATCH to setOf(DELETE_BATCH),
            UPSERT_TEMPORAL to setOf(UPSERT_TEMPORAL),
            APPEND_ATTRS_TEMPORAL to setOf(APPEND_ATTRS_TEMPORAL),
            DELETE_ATTRS_TEMPORAL to setOf(DELETE_ATTRS_TEMPORAL),
            UPDATE_ATTRINSTANCE_TEMPORAL to setOf(UPDATE_ATTRINSTANCE_TEMPORAL),
            DELETE_ATTRINSTANCE_TEMPORAL to setOf(DELETE_ATTRINSTANCE_TEMPORAL),
            DELETE_TEMPORAL to setOf(DELETE_TEMPORAL),
            MERGE_ENTITY to setOf(MERGE_ENTITY, REDIRECTION_OPS),
            REPLACE_ENTITY to setOf(REPLACE_ENTITY, REDIRECTION_OPS),
            REPLACE_ATTRS to setOf(REPLACE_ATTRS, UPDATE_OPS, REDIRECTION_OPS),
            RETRIEVE_ENTITY to setOf(RETRIEVE_ENTITY, RETRIEVE_OPS, FEDERATION_OPS, REDIRECTION_OPS),
            QUERY_ENTITY to setOf(QUERY_ENTITY, RETRIEVE_OPS, FEDERATION_OPS, REDIRECTION_OPS),
            QUERY_BATCH to setOf(QUERY_BATCH, QUERY_BATCH),
            RETRIEVE_TEMPORAL to setOf(RETRIEVE_TEMPORAL),
            QUERY_TEMPORAL to setOf(QUERY_TEMPORAL),
            RETRIEVE_ENTITY_TYPES to setOf(RETRIEVE_ENTITY_TYPES, FEDERATION_OPS, REDIRECTION_OPS),
            RETRIEVE_ENTITY_TYPE_DETAILS to setOf(RETRIEVE_ENTITY_TYPE_DETAILS, FEDERATION_OPS, REDIRECTION_OPS),
            RETRIEVE_ENTITY_TYPE_INFO to setOf(RETRIEVE_ENTITY_TYPE_INFO, FEDERATION_OPS, REDIRECTION_OPS),
            RETRIEVE_ATTR_TYPES to setOf(RETRIEVE_ATTR_TYPES, FEDERATION_OPS, REDIRECTION_OPS),
            RETRIEVE_ATTR_TYPE_DETAILS to setOf(RETRIEVE_ATTR_TYPE_DETAILS, FEDERATION_OPS, REDIRECTION_OPS),
            RETRIEVE_ATTR_TYPE_INFO to setOf(RETRIEVE_ATTR_TYPE_INFO, FEDERATION_OPS, REDIRECTION_OPS),
            CREATE_SUBSCRIPTION to setOf(CREATE_SUBSCRIPTION, FEDERATION_OPS),
            UPDATE_SUBSCRIPTION to setOf(UPDATE_SUBSCRIPTION, FEDERATION_OPS),
            RETRIEVE_SUBSCRIPTION to setOf(RETRIEVE_SUBSCRIPTION, FEDERATION_OPS),
            QUERY_SUBSCRIPTION to setOf(QUERY_SUBSCRIPTION, FEDERATION_OPS),
            DELETE_SUBSCRIPTION to setOf(DELETE_SUBSCRIPTION, FEDERATION_OPS),
        )

        fun fromString(operation: String): Operation? =
            Operation.entries.find { it.key == operation }
    }
}
