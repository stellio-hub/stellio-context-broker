package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_COMPACTED_ENTITY_MANDATORY_FIELDS
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntityEventListenerService(
    private val notificationService: NotificationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    @KafkaListener(topics = ["cim.entity._CatchAll"], groupId = "context_subscription")
    fun processMessage(content: String) {
        logger.debug("Received event: $content")
        coroutineScope.launch {
            dispatchEntityEvent(content)
        }
    }

    internal suspend fun dispatchEntityEvent(content: String) {
        kotlin.runCatching {
            val entityEvent = deserializeAs<EntityEvent>(content)
            when (entityEvent) {
                is EntityCreateEvent -> handleEntityEvent(
                    deserializeObject(entityEvent.operationPayload)
                        .keys
                        .filter { !JSONLD_COMPACTED_ENTITY_MANDATORY_FIELDS.contains(it) }
                        .toSet(),
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
                else -> OperationNotSupportedException(unhandledOperationType(entityEvent.operationType)).left()
            }.fold({
                if (it is OperationNotSupportedException)
                    logger.info(it.message)
                else
                    logger.error(entityEvent.failedHandlingMessage(it))
            }, {
                logger.debug(entityEvent.successfulHandlingMessage())
            })
        }.onFailure {
            logger.warn("Entity event processing has failed: ${it.message}", it)
        }
    }

    private suspend fun handleEntityEvent(
        updatedAttributes: Set<String>,
        entityPayload: String,
        contexts: List<String>
    ): Either<APIException, Unit> {
        logger.debug("Attributes considered in the event: $updatedAttributes")
        val jsonLdEntity = JsonLdUtils.expandJsonLdEntity(entityPayload, contexts)
        return notificationService.notifyMatchingSubscribers(
            jsonLdEntity,
            jsonLdEntity.toNgsiLdEntity(),
            updatedAttributes.map { JsonLdUtils.expandJsonLdTerm(it, contexts) }.toSet()
        ).onRight { results ->
            val succeeded = results.count { it.third }
            val failed = results.count { !it.third }
            logger.debug("Notified ${results.size} subscribers (success : $succeeded / failure : $failed)")
        }.map { Unit.right() }
    }
}
