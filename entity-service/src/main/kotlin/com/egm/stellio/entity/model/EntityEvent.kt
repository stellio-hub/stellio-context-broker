package com.egm.stellio.entity.model

data class EntityEvent(
    val operationType: EventType,
    val entityId: String,
    val entityType: String,
    val payload: String? = null,
    val updatedEntity: String? = null
)

enum class EventType {
    CREATE,
    UPDATE,
    APPEND,
    DELETE
}