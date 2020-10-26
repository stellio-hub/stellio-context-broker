package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.parseJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.parseEntityEvent
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
        when (val subscriptionEvent = parseEntityEvent(content)) {
            is EntityCreateEvent -> {
                val parsedSubscription = parseJsonLdFragment(subscriptionEvent.operationPayload)
                    .minus("id").minus("type")
                subscriptionHandlerService.createSubscriptionEntity(
                    subscriptionEvent.entityId, "Subscription", parsedSubscription
                )
            }
            is EntityUpdateEvent ->
                logger.warn("Subscription update operation is not yet implemented")
            is EntityDeleteEvent ->
                logger.warn("Subscription delete operation is not yet implemented")
        }
    }

    @StreamListener("cim.notification")
    fun processNotification(content: String) {
        when (val notificationEvent = parseEntityEvent(content)) {
            is EntityCreateEvent -> {
                var parsedNotification = parseJsonLdFragment(notificationEvent.operationPayload)
                val subscriptionId = (parsedNotification["subscriptionId"] as String).toUri()
                parsedNotification = parsedNotification.minus("id").minus("type").minus("subscriptionId")

                subscriptionHandlerService.createNotificationEntity(
                    notificationEvent.entityId,
                    "Notification",
                    subscriptionId,
                    parsedNotification
                )
            }
            else ->
                logger.warn(
                    "Received unexpected event type ${notificationEvent.operationType} " +
                        "for notification ${notificationEvent.entityId}"
                )
        }
    }
}
