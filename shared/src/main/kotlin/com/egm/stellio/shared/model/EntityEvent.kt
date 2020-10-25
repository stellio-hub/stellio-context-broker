package com.egm.stellio.shared.model

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import java.net.URI

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "operationType")
@JsonSubTypes(
    *[
        JsonSubTypes.Type(value = EntityCreateEvent::class),
        JsonSubTypes.Type(value = EntityDeleteEvent::class),
        JsonSubTypes.Type(value = AttributeAppendEvent::class),
        JsonSubTypes.Type(value = AttributeReplaceEvent::class),
        JsonSubTypes.Type(value = AttributeUpdateEvent::class),
        JsonSubTypes.Type(value = AttributeDeleteEvent::class)
    ]
)
open class EntityEvent(
    val operationType: EventsType,
    open val entityId: URI
) {
    @JsonIgnore
    open fun getEntity(): String? = null
}

@JsonTypeName("ENTITY_CREATE")
data class EntityCreateEvent(
    override val entityId: URI,
    val operationPayload: String
) : EntityEvent(EventsType.ENTITY_CREATE, entityId) {
    override fun getEntity() = this.operationPayload
}

@JsonTypeName("ENTITY_DELETE")
data class EntityDeleteEvent(override val entityId: URI) : EntityEvent(EventsType.ENTITY_DELETE, entityId)

@JsonTypeName("ATTRIBUTE_APPEND")
data class AttributeAppendEvent(
    override val entityId: URI,
    val attributeName: String,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String,
    val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_APPEND, entityId) {
    override fun getEntity() = this.updatedEntity
}

@JsonTypeName("ATTRIBUTE_REPLACE")
data class AttributeReplaceEvent(
    override val entityId: URI,
    val attributeName: String,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String,
    val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_REPLACE, entityId) {
    override fun getEntity() = this.updatedEntity
}

@JsonTypeName("ATTRIBUTE_UPDATE")
data class AttributeUpdateEvent(
    override val entityId: URI,
    val attributeName: String,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String,
    val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_UPDATE, entityId) {
    override fun getEntity() = this.updatedEntity
}

@JsonTypeName("ATTRIBUTE_DELETE")
data class AttributeDeleteEvent(
    override val entityId: URI,
    val attributeName: String,
    val datasetId: URI?,
    val updatedEntity: String,
    val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_DELETE, entityId)

enum class EventsType {
    ENTITY_CREATE,
    ENTITY_DELETE,
    ATTRIBUTE_APPEND,
    ATTRIBUTE_REPLACE,
    ATTRIBUTE_UPDATE,
    ATTRIBUTE_DELETE
}
