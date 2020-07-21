package com.egm.stellio.shared.model

data class EntityEvent(
    val operationType: EventType,
    val entityId: String,
    // TOOD not so nice because we expect it to be not null once sent
    val entityType: String? = null,
    val payload: String? = null,
    val updatedEntity: String? = null
)

enum class EventType {
    CREATE,
    UPDATE,
    APPEND,
    DELETE
}
