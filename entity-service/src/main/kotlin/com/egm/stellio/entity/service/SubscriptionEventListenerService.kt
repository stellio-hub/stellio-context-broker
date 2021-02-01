package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.toUri
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(SubscriptionSink::class)
class SubscriptionEventListenerService(
    private val subscriptionHandlerService: SubscriptionHandlerService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @StreamListener("cim.subscription")
    fun processSubscription(content: String) {
        when (val subscriptionEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleSubscriptionCreateEvent(subscriptionEvent)
            is EntityUpdateEvent -> logger.warn("Subscription update operation is not yet implemented")
            is EntityDeleteEvent -> logger.warn("Subscription delete operation is not yet implemented")
        }
    }

    @StreamListener("cim.notification")
    fun processNotification(content: String) {
        when (val notificationEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleNotificationCreateEvent(notificationEvent)
            else -> logger.warn(
                "Received unexpected event type ${notificationEvent.operationType} " +
                    "for notification ${notificationEvent.entityId}"
            )
        }
    }

    private fun handleSubscriptionCreateEvent(subscriptionCreateEvent: EntityCreateEvent) {
        val parsedSubscription = deserializeObject(subscriptionCreateEvent.operationPayload)
            .minus("id").minus("type")
        subscriptionHandlerService.createSubscriptionEntity(
            subscriptionCreateEvent.entityId, "Subscription", parsedSubscription
        )
    }

    private fun handleNotificationCreateEvent(notificationCreateEvent: EntityCreateEvent) {
        var parsedNotification = deserializeObject(notificationCreateEvent.operationPayload)
        val subscriptionId = (parsedNotification["subscriptionId"] as String).toUri()
        parsedNotification = parsedNotification.minus("id").minus("type").minus("subscriptionId")

        subscriptionHandlerService.createNotificationEntity(
            notificationCreateEvent.entityId,
            "Notification",
            subscriptionId,
            parsedNotification
        )
    }
}
