package com.egm.stellio.shared.model

import java.net.URI

sealed class EntitiesEvent(
    val operationType: EventsType,
    open val entityId: URI,
    open val attributeName: String? = null,
    open val datasetId: String? = null,
    open val operationPayload: String? = null,
    open val updatedEntity: String? = null,
    open val contexts: List<String>? = null
)

data class EntityCreateEvent(
    override val entityId: URI,
    override val operationPayload: String
) : EntitiesEvent(operationType = EventsType.ENTITY_CREATE, entityId = entityId, operationPayload = operationPayload)

data class EntityDeleteEvent(override val entityId: URI) : EntitiesEvent(
    operationType = EventsType.ENTITY_DELETE,
    entityId = entityId
)

data class AttributeAppendEvent(
    override val entityId: URI,
    override val attributeName: String,
    override val datasetId: String,
    override val operationPayload: String,
    override val updatedEntity: String,
    override val contexts: List<String>
) : EntitiesEvent(
    operationType = EventsType.ATTRIBUTE_APPEND,
    entityId = entityId,
    attributeName = attributeName,
    datasetId = datasetId,
    operationPayload = operationPayload,
    updatedEntity = updatedEntity,
    contexts = contexts
)

data class AttributeReplaceEvent(
    override val entityId: URI,
    override val attributeName: String,
    override val datasetId: String,
    override val operationPayload: String,
    override val updatedEntity: String,
    override val contexts: List<String>
) : EntitiesEvent(
    operationType = EventsType.ATTRIBUTE_REPLACE,
    entityId = entityId,
    attributeName = attributeName,
    datasetId = datasetId,
    operationPayload = operationPayload,
    updatedEntity = updatedEntity,
    contexts = contexts
)

data class AttributeUpdateEvent(
    override val entityId: URI,
    override val attributeName: String,
    override val datasetId: String,
    override val operationPayload: String,
    override val updatedEntity: String,
    override val contexts: List<String>
) : EntitiesEvent(
    operationType = EventsType.ATTRIBUTE_UPDATE,
    entityId = entityId,
    attributeName = attributeName,
    datasetId = datasetId,
    operationPayload = operationPayload,
    updatedEntity = updatedEntity,
    contexts = contexts
)

data class AttributeDeleteEvent(
    override val entityId: URI,
    override val attributeName: String,
    override val datasetId: String,
    override val updatedEntity: String,
    override val contexts: List<String>
) : EntitiesEvent(
    operationType = EventsType.ATTRIBUTE_DELETE,
    entityId = entityId,
    attributeName = attributeName,
    datasetId = datasetId,
    updatedEntity = updatedEntity,
    contexts = contexts
)

enum class EventsType {
    ENTITY_CREATE,
    ENTITY_DELETE,
    ATTRIBUTE_APPEND,
    ATTRIBUTE_REPLACE,
    ATTRIBUTE_UPDATE,
    ATTRIBUTE_DELETE
}
