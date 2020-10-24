package com.egm.stellio.shared.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import java.net.URI

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "operationType")
@JsonSubTypes(*[JsonSubTypes.Type(value = NotificationCreateEvent::class)])
open class NotificationEvent(
    val operationType: NotificationEventType,
    open val notificationId: URI
)

@JsonTypeName("NOTIFICATION_CREATE")
data class NotificationCreateEvent(
    override val notificationId: URI,
    val operationPayload: String
) : NotificationEvent(NotificationEventType.NOTIFICATION_CREATE, notificationId)

enum class NotificationEventType {
    NOTIFICATION_CREATE
}
