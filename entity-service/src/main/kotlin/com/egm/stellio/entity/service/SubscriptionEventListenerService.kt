package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EntityUpdateEvent
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.toUri
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class SubscriptionEventListenerService(
    private val subscriptionHandlerService: SubscriptionHandlerService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["cim.subscription"], groupId = "entity_service_subscription")
    fun processSubscription(content: String) {
        when (val subscriptionEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleSubscriptionCreateEvent(subscriptionEvent)
            is EntityUpdateEvent -> logger.warn("Subscription update operation is not yet implemented")
            is EntityDeleteEvent -> handleSubscriptionDeleteEvent(subscriptionEvent)
        }
    }

    @KafkaListener(topics = ["cim.notification"], groupId = "entity_service_notification")
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

    private fun handleSubscriptionDeleteEvent(subscriptionDeleteEvent: EntityDeleteEvent) =
        subscriptionHandlerService.deleteSubscriptionEntity(subscriptionDeleteEvent.entityId)

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
