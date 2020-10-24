package com.egm.stellio.shared.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import java.net.URI

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "operationType")
@JsonSubTypes(
    *[
        JsonSubTypes.Type(value = SubscriptionCreateEvent::class),
        JsonSubTypes.Type(value = SubscriptionUpdateEvent::class),
        JsonSubTypes.Type(value = SubscriptionDeleteEvent::class)
    ]
)
open class SubscriptionEvent(
    val operationType: SubscriptionEventType,
    open val subscriptionId: URI
)

@JsonTypeName("SUBSCRIPTION_CREATE")
data class SubscriptionCreateEvent(
    override val subscriptionId: URI,
    val operationPayload: String
) : SubscriptionEvent(SubscriptionEventType.SUBSCRIPTION_CREATE, subscriptionId)

@JsonTypeName("SUBSCRIPTION_UPDATE")
data class SubscriptionUpdateEvent(
    override val subscriptionId: URI,
    val operationPayload: String,
    val updatedSubscription: String,
    val contexts: List<String>
) : SubscriptionEvent(SubscriptionEventType.SUBSCRIPTION_UPDATE, subscriptionId)

@JsonTypeName("SUBSCRIPTION_DELETE")
data class SubscriptionDeleteEvent(override val subscriptionId: URI) :
    SubscriptionEvent(SubscriptionEventType.SUBSCRIPTION_DELETE, subscriptionId)

enum class SubscriptionEventType {
    SUBSCRIPTION_CREATE,
    SUBSCRIPTION_UPDATE,
    SUBSCRIPTION_DELETE
}
