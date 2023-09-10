package com.egm.stellio.subscription.listener

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_EXPANDED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.service.NotificationService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import reactor.core.Disposable
import java.net.URI

@Component
class EntityEventListenerService(
    private val notificationService: NotificationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    @KafkaListener(topics = ["cim.entity._CatchAll"], groupId = "context_subscription")
    fun processMessage(content: String) {
        coroutineScope.launch {
            dispatchEntityEvent(content)
        }
    }

    internal suspend fun dispatchEntityEvent(content: String) {
        kotlin.runCatching {
            val entityEvent = deserializeAs<EntityEvent>(content)
            val tenantUri = entityEvent.tenantUri
            logger.debug(
                "Handling {} event for entity {} in tenant {}",
                entityEvent.operationType,
                entityEvent.entityId,
                entityEvent.tenantUri
            )
            when (entityEvent) {
                is EntityCreateEvent -> handleEntityEvent(
                    tenantUri,
                    deserializeObject(entityEvent.operationPayload)
                        .keys
                        .filter { !JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.contains(it) }
                        .toSet(),
                    entityEvent.getEntity(),
                    entityEvent.contexts
                )
                is AttributeAppendEvent -> handleEntityEvent(
                    tenantUri,
                    setOf(entityEvent.attributeName),
                    entityEvent.getEntity(),
                    entityEvent.contexts
                )
                is AttributeReplaceEvent -> handleEntityEvent(
                    tenantUri,
                    setOf(entityEvent.attributeName),
                    entityEvent.getEntity(),
                    entityEvent.contexts
                )
                is AttributeUpdateEvent -> handleEntityEvent(
                    tenantUri,
                    setOf(entityEvent.attributeName),
                    entityEvent.getEntity(),
                    entityEvent.contexts
                )
                else -> OperationNotSupportedException(unhandledOperationType(entityEvent.operationType)).left()
            }
        }.onFailure {
            logger.warn("Entity event processing has failed: ${it.message}", it)
        }
    }

    private suspend fun handleEntityEvent(
        tenantUri: URI,
        updatedAttributes: Set<ExpandedTerm>,
        entityPayload: String,
        contexts: List<String>
    ): Either<APIException, Disposable> = either {
        logger.debug("Attributes considered in the event: {}", updatedAttributes)
        val jsonLdEntity = JsonLdEntity(entityPayload.deserializeAsMap(), contexts)
        mono {
            notificationService.notifyMatchingSubscribers(
                jsonLdEntity,
                jsonLdEntity.toNgsiLdEntity().bind(),
                updatedAttributes
            )
        }.contextWrite {
            it.put(NGSILD_TENANT_HEADER, tenantUri)
        }.subscribe { notificationResult ->
            notificationResult.fold({
                if (it is OperationNotSupportedException)
                    logger.info(it.message)
                else
                    logger.error("Error when trying to notifiy subscribers: {}", it.message, it)
            }, { results ->
                val totalNotifications = results.size
                val succeeded = results.count { it.third }
                val failed = results.count { !it.third }
                logger.debug("Notified $totalNotifications subscribers (success : $succeeded / failure : $failed)")
            })
        }
    }
}
