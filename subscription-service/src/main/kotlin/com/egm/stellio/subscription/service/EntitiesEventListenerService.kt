package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils.parseEntitiesEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntitiesEventListenerService(
    private val notificationService: NotificationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener, couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_subscription")
    fun processMessage(content: String) {
        val entityEvent = parseEntitiesEvent(content)
        val entity = entityEvent.getEntity()
        entity?.let {
            try {
                val updatedFragment = JsonLdUtils.parseJsonLdFragment(
                    entityEvent.getEventPayload()
                        ?: throw UnsupportedEventTypeException("Received unsupported event type")
                )
                val parsedEntity = JsonLdUtils.expandJsonLdEntity(it)
                notificationService.notifyMatchingSubscribers(it, parsedEntity.toNgsiLdEntity(), updatedFragment.keys)
                    .subscribe {
                        val succeeded = it.filter { it.third }.size
                        val failed = it.filter { !it.third }.size
                        logger.debug("Notified ${it.size} subscribers (success : $succeeded / failure : $failed)")
                    }
            } catch (e: UnsupportedEventTypeException) {
                logger.error(e.message)
            } catch (e: BadRequestDataException) {
                logger.error("Received a non-parseable entity : $content", e)
            } catch (e: InvalidRequestException) {
                logger.error("Received a non-parseable entity : $content", e)
            }
        }
    }
}
