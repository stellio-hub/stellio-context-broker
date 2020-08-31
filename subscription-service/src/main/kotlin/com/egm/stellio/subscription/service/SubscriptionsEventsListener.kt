package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class SubscriptionsEventsListener(
    private val eventMessageService: EventMessageService
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val channelName = "cim.subscription"

    @Async
    @EventListener(condition = "#subscriptionEvent.entityType == 'Subscription'")
    fun handleSubscriptionEvent(subscriptionEvent: EntityEvent) {
        val result = when (subscriptionEvent.operationType) {
            EventType.CREATE -> eventMessageService.sendMessage(
                channelName,
                EventType.CREATE,
                subscriptionEvent.entityId,
                subscriptionEvent.entityType!!,
                subscriptionEvent.payload,
                null
            )
            EventType.UPDATE -> eventMessageService.sendMessage(
                channelName,
                EventType.UPDATE,
                subscriptionEvent.entityId,
                subscriptionEvent.entityType!!,
                subscriptionEvent.payload,
                subscriptionEvent.updatedEntity
            )
            EventType.DELETE -> eventMessageService.sendMessage(
                channelName,
                EventType.DELETE,
                subscriptionEvent.entityId,
                subscriptionEvent.entityType!!,
                null,
                null
            )
            else -> false
        }

        if (result)
            logger.debug("Subscription ${subscriptionEvent.entityId} sent to $channelName")
        else
            logger.warn("Unable to send subscription ${subscriptionEvent.entityId} to $channelName")
    }
}
