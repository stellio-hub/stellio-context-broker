package com.egm.stellio.shared.model

import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import java.net.URI

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "operationType")
@JsonSubTypes(
    value = [
        JsonSubTypes.Type(value = EntityCreateEvent::class),
        JsonSubTypes.Type(value = EntityReplaceEvent::class),
        JsonSubTypes.Type(value = EntityDeleteEvent::class),
        JsonSubTypes.Type(value = AttributeCreateEvent::class),
        JsonSubTypes.Type(value = AttributeUpdateEvent::class),
        JsonSubTypes.Type(value = AttributeDeleteEvent::class)
    ]
)
sealed class EntityEvent(
    val operationType: EventsType,
    open val sub: String?,
    open val tenantName: String = DEFAULT_TENANT_NAME,
    open val entityId: URI,
    open val entityTypes: List<ExpandedTerm>
) {
    @JsonIgnore
    open fun getEntity(): String? = null

    @JsonIgnore
    open fun getAttribute(): ExpandedTerm? = null

    @JsonIgnore
    fun successfulHandlingMessage(): String =
        "Successfully handled event ${this.operationType} for resource ${this.entityId}"

    @JsonIgnore
    fun failedHandlingMessage(throwable: Throwable): String =
        "Error while handling event ${this.operationType} for resource ${this.entityId}: $throwable"
}

@JsonTypeName("ENTITY_CREATE")
data class EntityCreateEvent(
    override val sub: String?,
    override val tenantName: String = DEFAULT_TENANT_NAME,
    override val entityId: URI,
    override val entityTypes: List<ExpandedTerm>,
    val operationPayload: String
) : EntityEvent(EventsType.ENTITY_CREATE, sub, tenantName, entityId, entityTypes) {
    override fun getEntity() = this.operationPayload
}

@JsonTypeName("ENTITY_REPLACE")
data class EntityReplaceEvent(
    override val sub: String?,
    override val tenantName: String = DEFAULT_TENANT_NAME,
    override val entityId: URI,
    override val entityTypes: List<ExpandedTerm>,
    val operationPayload: String
) : EntityEvent(EventsType.ENTITY_REPLACE, sub, tenantName, entityId, entityTypes) {
    override fun getEntity() = this.operationPayload
}

@JsonTypeName("ENTITY_DELETE")
data class EntityDeleteEvent(
    override val sub: String?,
    override val tenantName: String = DEFAULT_TENANT_NAME,
    override val entityId: URI,
    override val entityTypes: List<ExpandedTerm>,
    // null only when in the case of an IAM event (previous state is not known)
    val previousEntity: String?,
    val updatedEntity: String
) : EntityEvent(EventsType.ENTITY_DELETE, sub, tenantName, entityId, entityTypes) {
    override fun getEntity() = this.previousEntity
}

@JsonTypeName("ATTRIBUTE_CREATE")
data class AttributeCreateEvent(
    override val sub: String?,
    override val tenantName: String = DEFAULT_TENANT_NAME,
    override val entityId: URI,
    override val entityTypes: List<ExpandedTerm>,
    val attributeName: ExpandedTerm,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String
) : EntityEvent(EventsType.ATTRIBUTE_CREATE, sub, tenantName, entityId, entityTypes) {
    override fun getEntity() = this.updatedEntity
    override fun getAttribute() = this.attributeName
}

@JsonTypeName("ATTRIBUTE_UPDATE")
data class AttributeUpdateEvent(
    override val sub: String?,
    override val tenantName: String = DEFAULT_TENANT_NAME,
    override val entityId: URI,
    override val entityTypes: List<ExpandedTerm>,
    val attributeName: ExpandedTerm,
    val datasetId: URI?,
    val operationPayload: String,
    val updatedEntity: String
) : EntityEvent(EventsType.ATTRIBUTE_UPDATE, sub, tenantName, entityId, entityTypes) {
    override fun getEntity() = this.updatedEntity
    override fun getAttribute() = this.attributeName
}

@JsonTypeName("ATTRIBUTE_DELETE")
data class AttributeDeleteEvent(
    override val sub: String?,
    override val tenantName: String = DEFAULT_TENANT_NAME,
    override val entityId: URI,
    override val entityTypes: List<ExpandedTerm>,
    val attributeName: ExpandedTerm,
    val datasetId: URI?,
    val updatedEntity: String
) : EntityEvent(EventsType.ATTRIBUTE_DELETE, sub, tenantName, entityId, entityTypes) {
    override fun getEntity() = this.updatedEntity
    override fun getAttribute() = this.attributeName
}

enum class EventsType {
    ENTITY_CREATE,
    ENTITY_REPLACE,
    ENTITY_DELETE,
    ATTRIBUTE_CREATE,
    ATTRIBUTE_UPDATE,
    ATTRIBUTE_DELETE
}

fun unhandledOperationType(operationType: EventsType): String = "Entity event $operationType not handled."
