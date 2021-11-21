package com.egm.stellio.shared.model

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import java.net.URI

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "operationType")
@JsonSubTypes(
    JsonSubTypes.Type(value = EntityCreateEvent::class),
    JsonSubTypes.Type(value = EntityReplaceEvent::class),
    JsonSubTypes.Type(value = EntityUpdateEvent::class),
    JsonSubTypes.Type(value = EntityDeleteEvent::class),
    JsonSubTypes.Type(value = AttributeAppendEvent::class),
    JsonSubTypes.Type(value = AttributeReplaceEvent::class),
    JsonSubTypes.Type(value = AttributeUpdateEvent::class),
    JsonSubTypes.Type(value = AttributeDeleteEvent::class),
    JsonSubTypes.Type(value = AttributeDeleteAllInstancesEvent::class)
)
open class EntityEvent(
    val operationType: EventsType,
    open val entityId: URI,
    open val entityType: String,
    open val contexts: List<String>
) {
    @JsonIgnore
    open fun getEntity(): String? = null
}

@JsonTypeName("ENTITY_CREATE")
data class EntityCreateEvent(
    override val entityId: URI,
    override val entityType: String,
    val operationPayload: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ENTITY_CREATE, entityId, entityType, contexts) {
    override fun getEntity() = this.operationPayload
}

@JsonTypeName("ENTITY_REPLACE")
data class EntityReplaceEvent(
    override val entityId: URI,
    override val entityType: String,
    val operationPayload: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ENTITY_REPLACE, entityId, entityType, contexts) {
    override fun getEntity() = this.operationPayload
}

@JsonTypeName("ENTITY_UPDATE")
data class EntityUpdateEvent(
    override val entityId: URI,
    override val entityType: String,
    val operationPayload: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ENTITY_UPDATE, entityId, entityType, contexts)

@JsonTypeName("ENTITY_DELETE")
data class EntityDeleteEvent(
    override val entityId: URI,
    override val entityType: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ENTITY_DELETE, entityId, entityType, contexts)

@JsonTypeName("ATTRIBUTE_APPEND")
data class AttributeAppendEvent(
    override val entityId: URI,
    override val entityType: String,
    val attributeName: String,
    val datasetId: URI?,
    val overwrite: Boolean = true,
    val operationPayload: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_APPEND, entityId, entityType, contexts) {
    override fun getEntity() = this.updatedEntity
}

@JsonTypeName("ATTRIBUTE_REPLACE")
data class AttributeReplaceEvent(
    override val entityId: URI,
    override val entityType: String,
    val attributeName: String,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_REPLACE, entityId, entityType, contexts) {
    override fun getEntity() = this.updatedEntity
}

@JsonTypeName("ATTRIBUTE_UPDATE")
data class AttributeUpdateEvent(
    override val entityId: URI,
    override val entityType: String,
    val attributeName: String,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_UPDATE, entityId, entityType, contexts) {
    override fun getEntity() = this.updatedEntity
}

@JsonTypeName("ATTRIBUTE_DELETE")
data class AttributeDeleteEvent(
    override val entityId: URI,
    override val entityType: String,
    val attributeName: String,
    val datasetId: URI?,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_DELETE, entityId, entityType, contexts)

@JsonTypeName("ATTRIBUTE_DELETE_ALL_INSTANCES")
data class AttributeDeleteAllInstancesEvent(
    override val entityId: URI,
    override val entityType: String,
    val attributeName: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_DELETE_ALL_INSTANCES, entityId, entityType, contexts)

enum class EventsType {
    ENTITY_CREATE,
    ENTITY_REPLACE,
    ENTITY_UPDATE,
    ENTITY_DELETE,
    ATTRIBUTE_APPEND,
    ATTRIBUTE_REPLACE,
    ATTRIBUTE_UPDATE,
    ATTRIBUTE_DELETE,
    ATTRIBUTE_DELETE_ALL_INSTANCES
}
