package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component

@Component
class NotificationsEventsListener(
    private val eventMessageService: EventMessageService
) {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val channelName = "cim.notification"

    @Async
    @EventListener(condition = "#notificationEvent.entityType == 'Notification'")
    fun handleNotificationEvent(notificationEvent: EntityEvent) {

        val result = when (notificationEvent.operationType) {
            EventType.CREATE -> eventMessageService.sendMessage(
                channelName,
                EventType.CREATE,
                notificationEvent.entityId,
                notificationEvent.entityType,
                notificationEvent.payload,
                null
            )
            else -> {
                logger.warn("${notificationEvent.operationType} operation not supported")
                return
            }
        }

        if (result)
            logger.debug("Notification ${notificationEvent.entityId} sent to $channelName")
        else
            logger.warn("Unable to send notification ${notificationEvent.entityId} to $channelName")
    }
}
