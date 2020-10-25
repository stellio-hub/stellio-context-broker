package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.parseJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.parseNotificationEvent
import com.egm.stellio.shared.util.JsonUtils.parseSubscriptionEvent
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
        when (val subscriptionEvent = parseSubscriptionEvent(content)) {
            is SubscriptionCreateEvent -> {
                val parsedSubscription = parseJsonLdFragment(subscriptionEvent.operationPayload)
                    .minus("id").minus("type")
                subscriptionHandlerService.createSubscriptionEntity(
                    subscriptionEvent.subscriptionId, "Subscription", parsedSubscription
                )
            }
            is SubscriptionUpdateEvent ->
                logger.warn("Subscription update operation is not yet implemented")
            is SubscriptionDeleteEvent ->
                logger.warn("Subscription delete operation is not yet implemented")
        }
    }

    @StreamListener("cim.notification")
    fun processNotification(content: String) {
        when (val notificationEvent = parseNotificationEvent(content)) {
            is NotificationCreateEvent -> {
                var parsedNotification = parseJsonLdFragment(notificationEvent.operationPayload)
                val subscriptionId = (parsedNotification["subscriptionId"] as String).toUri()
                parsedNotification = parsedNotification.minus("id").minus("type").minus("subscriptionId")

                subscriptionHandlerService.createNotificationEntity(
                    notificationEvent.notificationId,
                    "Notification",
                    subscriptionId,
                    parsedNotification
                )
            }
            else ->
                logger.warn(
                    "Received unexpected event type ${notificationEvent.operationType} " +
                        "for notification ${notificationEvent.notificationId}"
                )
        }
    }
}
