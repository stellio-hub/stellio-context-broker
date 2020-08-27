package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.parseJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.parseEntityEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntitiesListener(
    private val notificationService: NotificationService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener, couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_subscription")
    fun processMessage(content: String) {
        val entityEvent = parseEntityEvent(content)
        val entity = getEntityFromEvent(entityEvent)
        entity?.let {
            try {
                val updatedFragment = parseJsonLdFragment(entityEvent.payload!!)
                val parsedEntity = expandJsonLdEntity(it)
                notificationService.notifyMatchingSubscribers(it, parsedEntity.toNgsiLdEntity(), updatedFragment.keys)
                    .subscribe {
                        val succeeded = it.filter { it.third }.size
                        val failed = it.filter { !it.third }.size
                        logger.debug("Notified ${it.size} subscribers (success : $succeeded / failure : $failed)")
                    }
            } catch (e: Exception) {
                logger.error("Received a non-parseable entity : $content", e)
            }
        }
    }

    private fun getEntityFromEvent(entityEvent: EntityEvent): String? {
        return when (entityEvent.operationType) {
            EventType.CREATE -> {
                entityEvent.payload
            }
            EventType.UPDATE -> {
                entityEvent.updatedEntity
            }
            EventType.APPEND -> {
                logger.warn("Append operation is not yet implemented")
                null
            }
            EventType.DELETE -> {
                logger.warn("Delete operation is not yet implemented")
                null
            }
        }
    }
}
