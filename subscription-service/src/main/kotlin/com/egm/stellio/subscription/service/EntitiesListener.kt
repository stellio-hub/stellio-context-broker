package com.egm.stellio.subscription.service

import com.egm.stellio.subscription.model.EventType
import com.egm.stellio.subscription.utils.NgsiLdParsingUtils
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntitiesListener(
    private val notificationService: NotificationService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener as I couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entities.*", groupId = "context_subscription")
    fun processMessage(content: String) {
        val entityEvent = NgsiLdParsingUtils.parseEntityEvent(content)
        if (entityEvent.operationType == EventType.UPDATE) {
            entityEvent.updatedEntity?.let {
                try {
                    val entity = NgsiLdParsingUtils.parseEntity(it)
                    notificationService.notifyMatchingSubscribers(content, entity)
                            .subscribe {
                                val succeededNotifications = it.filter { it.third }.size
                                val failedNotifications = it.filter { !it.third }.size
                                logger.debug("Notified ${it.size} subscribers (success : $succeededNotifications / failure : $failedNotifications)")
                            }
                } catch (e: Exception) {
                    logger.error("Received a non-parseable entity : $content", e)
                }
            }
        }
    }
}