package com.egm.stellio.search.csr.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.springframework.http.HttpMethod
import java.net.URI

enum class Operation(
    val key: String,
    private val matchingOperationGroups: Set<Operation> = emptySet(),
    val method: HttpMethod? = null,
    private val path: NGSILDPath? = null
) {

    // OPERATION GROUPS
    @JsonProperty("federationOps")
    FEDERATION_OPS("federationOps"),

    @JsonProperty("updateOps")
    UPDATE_OPS("updateOps"),

    @JsonProperty("retrieveOps")
    RETRIEVE_OPS("retrieveOps"),

    @JsonProperty("redirectionOps")
    REDIRECTION_OPS("redirectionOps"),

    @JsonProperty("createEntity")
    CREATE_ENTITY(
        "createEntity",
        setOf(REDIRECTION_OPS),
        HttpMethod.POST,
        NGSILDPath.ENTITIES
    ),

    // not implemented
    @JsonProperty("updateEntity")
    UPDATE_ENTITY( // update attributes
        "updateEntity",
        setOf(UPDATE_OPS, REDIRECTION_OPS),
        HttpMethod.PATCH,
        NGSILDPath.ATTRIBUTES
    ),

    // not implemented
    @JsonProperty("appendAttrs")
    APPEND_ATTRS(
        "appendAttrs",
        setOf(REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("updateAttrs")
    UPDATE_ATTRS(
        "updateAttrs",
        setOf(UPDATE_OPS, REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("deleteAttrs")
    DELETE_ATTRS(
        "deleteAttrs",
        setOf(REDIRECTION_OPS)
    ),

    @JsonProperty("deleteEntity")
    DELETE_ENTITY(
        "deleteEntity",
        setOf(REDIRECTION_OPS),
        HttpMethod.DELETE,
        NGSILDPath.ENTITIES
    ),

    // not implemented
    @JsonProperty("createBatch")
    CREATE_BATCH("createBatch"),

    // not implemented
    @JsonProperty("upsertBatch")
    UPSERT_BATCH("upsertBatch"),

    // not implemented
    @JsonProperty("updateBatch")
    UPDATE_BATCH("updateBatch"),

    // not implemented
    @JsonProperty("deleteBatch")
    DELETE_BATCH("deleteBatch"),

    // not implemented
    @JsonProperty("upsertTemporal")
    UPSERT_TEMPORAL("upsertTemporal"),

    // not implemented
    @JsonProperty("appendAttrsTemporal")
    APPEND_ATTRS_TEMPORAL("appendAttrsTemporal"),

    // not implemented
    @JsonProperty("deleteAttrsTemporal")
    DELETE_ATTRS_TEMPORAL("deleteAttrsTemporal"),

    // not implemented
    @JsonProperty("updateAttrInstanceTemporal")
    UPDATE_ATTRINSTANCE_TEMPORAL("updateAttrInstanceTemporal"),

    // not implemented
    @JsonProperty("deleteAttrInstanceTemporal")
    DELETE_ATTRINSTANCE_TEMPORAL("deleteAttrInstanceTemporal"),

    // not implemented
    @JsonProperty("deleteTemporal")
    DELETE_TEMPORAL("deleteTemporal"),

    // not implemented
    @JsonProperty("mergeEntity")
    MERGE_ENTITY(
        "mergeEntity",
        setOf(REDIRECTION_OPS),
        HttpMethod.PATCH,
        NGSILDPath.ENTITY
    ),

    // not implemented
    @JsonProperty("replaceEntity")
    REPLACE_ENTITY(
        "replaceEntity",
        setOf(UPDATE_OPS, REDIRECTION_OPS),
        HttpMethod.PUT,
        NGSILDPath.ENTITY
    ),

    // not implemented
    @JsonProperty("replaceAttrs")
    REPLACE_ATTRS(
        "replaceAttrs",
        setOf(UPDATE_OPS, REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("mergeBatch")
    MERGE_BATCH("mergeBatch"),

    // CONSUMPTION
    @JsonProperty("retrieveEntity")
    RETRIEVE_ENTITY(
        "retrieveEntity",
        setOf(RETRIEVE_OPS, FEDERATION_OPS, REDIRECTION_OPS)
    ),

    @JsonProperty("queryEntity")
    QUERY_ENTITY(
        "queryEntity",
        setOf(RETRIEVE_OPS, FEDERATION_OPS, REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("queryBatch")
    QUERY_BATCH("queryBatch", setOf(FEDERATION_OPS)),

    // not implemented
    @JsonProperty("retrieveTemporal")
    RETRIEVE_TEMPORAL("retrieveTemporal"),

    // not implemented
    @JsonProperty("queryTemporal")
    QUERY_TEMPORAL("queryTemporal"),

    // not implemented
    @JsonProperty("retrieveEntityTypes")
    RETRIEVE_ENTITY_TYPES(
        "retrieveEntityTypes",
        setOf(FEDERATION_OPS, REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("retrieveEntityTypeDetails")
    RETRIEVE_ENTITY_TYPE_DETAILS(
        "retrieveEntityTypeDetails",
        setOf(FEDERATION_OPS, REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("retrieveEntityTypeInfo")
    RETRIEVE_ENTITY_TYPE_INFO(
        "retrieveEntityTypeInfo",
        setOf(FEDERATION_OPS, REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("retrieveAttrTypes")
    RETRIEVE_ATTR_TYPES(
        "retrieveAttrTypes",
        setOf(FEDERATION_OPS, REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("retrieveAttrTypeDetails")
    RETRIEVE_ATTR_TYPE_DETAILS(
        "retrieveAttrTypeDetails",
        setOf(FEDERATION_OPS, REDIRECTION_OPS)
    ),

    // not implemented
    @JsonProperty("retrieveAttrTypeInfo")
    RETRIEVE_ATTR_TYPE_INFO(
        "retrieveAttrTypeInfo",
        setOf(FEDERATION_OPS, REDIRECTION_OPS)
    ),

    // SUBSCRIPTION
    // not implemented
    @JsonProperty("createSubscription")
    CREATE_SUBSCRIPTION(
        "createSubscription",
        setOf(FEDERATION_OPS)
    ),

    // not implemented
    @JsonProperty("updateSubscription")
    UPDATE_SUBSCRIPTION(
        "updateSubscription",
        setOf(FEDERATION_OPS)
    ),

    // not implemented
    @JsonProperty("retrieveSubscription")
    RETRIEVE_SUBSCRIPTION(
        "retrieveSubscription",
        setOf(FEDERATION_OPS)
    ),

    // not implemented
    @JsonProperty("querySubscription")
    QUERY_SUBSCRIPTION(
        "querySubscription",
        setOf(FEDERATION_OPS)
    ),

    // not implemented
    @JsonProperty("deleteSubscription")
    DELETE_SUBSCRIPTION(
        "deleteSubscription",
        setOf(FEDERATION_OPS)
    );

    fun getMatchingOperations() = matchingOperationGroups + this
    fun getPath(entityId: URI? = null, attrId: String = "") = path?.pattern
        ?.replace(entityIdPlaceHolder, entityId.toString())
        ?.replace(attributeIdPlaceHolder, attrId)



    companion object {
        fun fromString(operation: String): Operation? =
            Operation.entries.find { it.key == operation }

        const val entityPath = "/ngsi-ld/v1/entities"
        const val entityIdPlaceHolder = ":entityId"
        const val attributeIdPlaceHolder = ":attrId"
        const val attrsPath = "$entityPath/$entityIdPlaceHolder/attrs"

        enum class NGSILDPath(val pattern: String){
            ENTITIES(entityPath),
            ENTITY("$entityPath/$entityIdPlaceHolder"),
            ATTRIBUTES(attrsPath),
            ATTRIBUTE("$attrsPath/$attributeIdPlaceHolder")
        }
    }
}
