package com.egm.datahub.context.registry.model

data class EntityEvent(
    val entityType: String,
    val entityUrn: String,
    val operation: EventType,
    val payload: String
)

enum class EventType {
    POST,
    PUT,
    PATCH,
    DELETE
}