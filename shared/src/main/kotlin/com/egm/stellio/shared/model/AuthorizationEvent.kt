package com.egm.stellio.shared.model

import java.net.URI

data class AuthorizationEvent(
    val operationType: EventType,
    val entityId: URI,
    // TODO not so nice because we expect it to be not null once sent
    val entityType: String? = null,
    val attributeName: String? = null,
    val datasetId: String? = null,
    val context: List<String>? = null,
    val payload: String? = null,
    val updatedEntity: String? = null
)

enum class EventType {
    CREATE,
    UPDATE,
    APPEND,
    DELETE
}
