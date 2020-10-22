package com.egm.stellio.shared.model

import java.net.URI

open class EntitiesEvent(
    val operationType: EventsType,
    open val entityId: URI
)

data class EntityCreateEvent(
    override val entityId: URI,
    val operationPayload: String
) : EntitiesEvent(EventsType.ENTITY_CREATE, entityId)

data class EntityDeleteEvent(override val entityId: URI) : EntitiesEvent(EventsType.ENTITY_DELETE, entityId)

data class AttributeAppendEvent(
    override val entityId: URI,
    val attributeName: String,
    val datasetId: String,
    val operationPayload: String,
    val updatedEntity: String,
    val contexts: List<String>
) : EntitiesEvent(EventsType.ATTRIBUTE_APPEND, entityId)

data class AttributeReplaceEvent(
    override val entityId: URI,
    val attributeName: String,
    val datasetId: String,
    val operationPayload: String,
    val updatedEntity: String,
    val contexts: List<String>
) : EntitiesEvent(EventsType.ATTRIBUTE_REPLACE, entityId)

data class AttributeUpdateEvent(
    override val entityId: URI,
    val attributeName: String,
    val datasetId: String,
    val operationPayload: String,
    val updatedEntity: String,
    val contexts: List<String>
) : EntitiesEvent(EventsType.ATTRIBUTE_UPDATE, entityId)

data class AttributeDeleteEvent(
    override val entityId: URI,
    val attributeName: String,
    val datasetId: String,
    val updatedEntity: String,
    val contexts: List<String>
) : EntitiesEvent(EventsType.ATTRIBUTE_DELETE, entityId)

enum class EventsType {
    ENTITY_CREATE,
    ENTITY_DELETE,
    ATTRIBUTE_APPEND,
    ATTRIBUTE_REPLACE,
    ATTRIBUTE_UPDATE,
    ATTRIBUTE_DELETE
}
