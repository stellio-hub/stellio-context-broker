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
        val operationGroups = mapOf(
            "federationOps" to listOf(
                RETRIEVE_ENTITY,
                QUERY_ENTITY,
                QUERY_BATCH,
                RETRIEVE_ENTITY_TYPES,
                RETRIEVE_ENTITY_TYPE_DETAILS,
                RETRIEVE_ENTITY_TYPE_INFO,
                RETRIEVE_ATTR_TYPES,
                RETRIEVE_ATTR_TYPE_DETAILS,
                RETRIEVE_ATTR_TYPE_INFO,
                CREATE_SUBSCRIPTION,
                UPDATE_SUBSCRIPTION,
                RETRIEVE_SUBSCRIPTION,
                QUERY_SUBSCRIPTION,
                DELETE_SUBSCRIPTION,
            ),
            "updateOps" to listOf(
                UPDATE_ENTITY,
                UPDATE_ATTRS,
                REPLACE_ENTITY,
                REPLACE_ATTRS
            ),
            "retrieveOps" to listOf(
                RETRIEVE_ENTITY,
                QUERY_ENTITY
            ),
            "redirectionOps" to listOf(
                CREATE_ENTITY,
                UPDATE_ENTITY,
                APPEND_ATTRS,
                UPDATE_ATTRS,
                DELETE_ATTRS,
                DELETE_ENTITY,
                MERGE_ENTITY,
                REPLACE_ENTITY,
                REPLACE_ATTRS,
                RETRIEVE_ENTITY,
                QUERY_ENTITY,
                RETRIEVE_ENTITY_TYPES,
                RETRIEVE_ENTITY_TYPE_DETAILS,
                RETRIEVE_ENTITY_TYPE_INFO,
                RETRIEVE_ATTR_TYPES,
                RETRIEVE_ATTR_TYPE_DETAILS,
                RETRIEVE_ATTR_TYPE_INFO
            )
        )
        fun fromString(operation: String): Operation? =
            Operation.entries.find { it.key == operation }
    }
}
