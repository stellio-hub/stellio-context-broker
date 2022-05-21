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

    @KafkaListener(topicPattern = "cim.entity._CatchAll", groupId = "context_subscription")
    fun processMessage(content: String) {
        val entityEvent = deserializeAs<EntityEvent>(content)
        logger.debug("Handling ${entityEvent.operationType} event on entity ${entityEvent.entityId}")
        when (entityEvent) {
            is EntityCreateEvent -> handleEntityEvent(
                deserializeObject(entityEvent.operationPayload).keys,
                entityEvent.getEntity(),
                entityEvent.contexts
            )
            is AttributeAppendEvent -> handleEntityEvent(
                setOf(entityEvent.attributeName),
                entityEvent.getEntity(),
                entityEvent.contexts
            )
            is AttributeReplaceEvent -> handleEntityEvent(
                setOf(entityEvent.attributeName),
                entityEvent.getEntity(),
                entityEvent.contexts
            )
            is AttributeUpdateEvent -> handleEntityEvent(
                setOf(entityEvent.attributeName),
                entityEvent.getEntity(),
                entityEvent.contexts
            )
            else -> logger.debug("Nothing to do for operation of type ${entityEvent.operationType}")
        }
    }

    private fun handleEntityEvent(updatedAttributes: Set<String>, entityPayload: String, contexts: List<String>) {
        logger.debug("Attributes considered in the event: $updatedAttributes")
        try {
            val parsedEntity = JsonLdUtils.expandJsonLdEntity(entityPayload, contexts)
            notificationService.notifyMatchingSubscribers(
                entityPayload,
                parsedEntity.toNgsiLdEntity(),
                updatedAttributes
            ).subscribe { results ->
                val succeeded = results.count { it.third }
                val failed = results.count { !it.third }
                logger.debug("Notified ${results.size} subscribers (success : $succeeded / failure : $failed)")
            }
        } catch (e: BadRequestDataException) {
            logger.error("Received a non-parsable entity", e)
        } catch (e: InvalidRequestException) {
            logger.error("Received a non-parsable entity", e)
        }
    }
}
