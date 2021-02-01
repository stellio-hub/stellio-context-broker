package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntityEventListenerService(
    private val notificationService: NotificationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener, couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_subscription")
    fun processMessage(content: String) {
        when (val entityEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleEntityEvent(
                entityEvent.operationPayload,
                entityEvent.getEntity(),
                entityEvent.contexts
            )
            is EntityDeleteEvent -> logger.warn("Entity delete operation is not yet implemented")
            is AttributeAppendEvent -> logger.warn("Attribute append operation is not yet implemented")
            is AttributeReplaceEvent -> handleEntityEvent(
                entityEvent.operationPayload,
                entityEvent.getEntity(),
                entityEvent.contexts
            )
            is AttributeUpdateEvent -> logger.warn("Attribute update operation is not yet implemented")
            is AttributeDeleteEvent -> logger.warn("Attribute delete operation is not yet implemented")
        }
    }

    private fun handleEntityEvent(eventPayload: String, entityPayload: String, contexts: List<String>) {
        try {
            val updatedFragment = deserializeObject(eventPayload)
            val parsedEntity = JsonLdUtils.expandJsonLdEntity(entityPayload, contexts)
            notificationService.notifyMatchingSubscribers(
                entityPayload,
                parsedEntity.toNgsiLdEntity(),
                updatedFragment.keys
            ).subscribe {
                val succeeded = it.filter { it.third }.size
                val failed = it.filter { !it.third }.size
                logger.debug("Notified ${it.size} subscribers (success : $succeeded / failure : $failed)")
            }
        } catch (e: BadRequestDataException) {
            logger.error("Received a non-parsable entity", e)
        } catch (e: InvalidRequestException) {
            logger.error("Received a non-parsable entity", e)
        }
    }
}
