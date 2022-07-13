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
        JsonSubTypes.Type(value = EntityReplaceEvent::class),
        JsonSubTypes.Type(value = EntityUpdateEvent::class),
        JsonSubTypes.Type(value = EntityDeleteEvent::class),
        JsonSubTypes.Type(value = AttributeAppendEvent::class),
        JsonSubTypes.Type(value = AttributeReplaceEvent::class),
        JsonSubTypes.Type(value = AttributeUpdateEvent::class),
        JsonSubTypes.Type(value = AttributeDeleteEvent::class),
        JsonSubTypes.Type(value = AttributeDeleteAllInstancesEvent::class)
    ]
)
open class EntityEvent(
    val operationType: EventsType,
    open val sub: String?,
    open val entityId: URI,
    open val entityTypes: List<String>,
    open val contexts: List<String>
) {
    @JsonIgnore
    open fun getEntity(): String? = null

    @JsonIgnore
    open fun getAttribute(): String? = null
}

@JsonTypeName("ENTITY_CREATE")
data class EntityCreateEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    val operationPayload: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ENTITY_CREATE, sub, entityId, entityTypes, contexts) {
    override fun getEntity() = this.operationPayload
}

@JsonTypeName("ENTITY_REPLACE")
data class EntityReplaceEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    val operationPayload: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ENTITY_REPLACE, sub, entityId, entityTypes, contexts) {
    override fun getEntity() = this.operationPayload
}

@JsonTypeName("ENTITY_UPDATE")
data class EntityUpdateEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    val operationPayload: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ENTITY_UPDATE, sub, entityId, entityTypes, contexts)

@JsonTypeName("ENTITY_DELETE")
data class EntityDeleteEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    override val contexts: List<String>
) : EntityEvent(EventsType.ENTITY_DELETE, sub, entityId, entityTypes, contexts)

@JsonTypeName("ATTRIBUTE_APPEND")
data class AttributeAppendEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    val attributeName: String,
    val datasetId: URI?,
    val overwrite: Boolean = true,
    val operationPayload: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_APPEND, sub, entityId, entityTypes, contexts) {
    override fun getEntity() = this.updatedEntity
    override fun getAttribute() = this.attributeName
}

@JsonTypeName("ATTRIBUTE_REPLACE")
data class AttributeReplaceEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    val attributeName: String,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_REPLACE, sub, entityId, entityTypes, contexts) {
    override fun getEntity() = this.updatedEntity
    override fun getAttribute() = this.attributeName
}

@JsonTypeName("ATTRIBUTE_UPDATE")
data class AttributeUpdateEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    val attributeName: String,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_UPDATE, sub, entityId, entityTypes, contexts) {
    override fun getEntity() = this.updatedEntity
    override fun getAttribute() = this.attributeName
}

@JsonTypeName("ATTRIBUTE_DELETE")
data class AttributeDeleteEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    val attributeName: String,
    val datasetId: URI?,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_DELETE, sub, entityId, entityTypes, contexts) {
    override fun getAttribute() = this.attributeName
}

@JsonTypeName("ATTRIBUTE_DELETE_ALL_INSTANCES")
data class AttributeDeleteAllInstancesEvent(
    override val sub: String?,
    override val entityId: URI,
    override val entityTypes: List<String>,
    val attributeName: String,
    val updatedEntity: String,
    override val contexts: List<String>
) : EntityEvent(EventsType.ATTRIBUTE_DELETE_ALL_INSTANCES, sub, entityId, entityTypes, contexts) {
    override fun getAttribute() = this.attributeName
}

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
