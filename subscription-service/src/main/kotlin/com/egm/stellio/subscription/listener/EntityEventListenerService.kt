package com.egm.stellio.subscription.listener

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AttributeCreateEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.OperationNotSupportedException
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_EXPANDED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SYSATTRS_PROPERTIES
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.model.NotificationTrigger
import com.egm.stellio.subscription.service.NotificationService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import reactor.core.Disposable

@Component
class EntityEventListenerService(
    private val notificationService: NotificationService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    private val ignoredExpandedEntityProperties = JSONLD_EXPANDED_ENTITY_CORE_MEMBERS.plus(NGSILD_SYSATTRS_PROPERTIES)

    @KafkaListener(topics = ["cim.entity._CatchAll"], groupId = "context_subscription")
    fun processMessage(content: String) {
        coroutineScope.launch {
            dispatchEntityEvent(content)
        }
    }

    internal suspend fun dispatchEntityEvent(content: String) {
        kotlin.runCatching {
            val entityEvent = deserializeAs<EntityEvent>(content)
            val tenantName = entityEvent.tenantName
            logger.debug(
                "Handling {} event for entity {} in tenant {}",
                entityEvent.operationType,
                entityEvent.entityId,
                entityEvent.tenantName
            )
            when (entityEvent) {
                is EntityCreateEvent -> handleEntityEvent(
                    tenantName,
                    entityEvent.operationPayload.getUpdatedAttributes(),
                    Pair(entityEvent.getEntity(), entityEvent.getEntity()),
                    NotificationTrigger.ENTITY_CREATED
                )
                is EntityDeleteEvent -> handleEntityEvent(
                    tenantName,
                    emptySet(),
                    Pair(entityEvent.getEntity()!!, entityEvent.updatedEntity),
                    NotificationTrigger.ENTITY_DELETED
                )
                is AttributeCreateEvent -> handleEntityEvent(
                    tenantName,
                    setOf(entityEvent.attributeName),
                    Pair(entityEvent.getEntity(), entityEvent.getEntity()),
                    NotificationTrigger.ATTRIBUTE_CREATED
                )
                is AttributeUpdateEvent -> handleEntityEvent(
                    tenantName,
                    setOf(entityEvent.attributeName),
                    Pair(entityEvent.getEntity(), entityEvent.getEntity()),
                    NotificationTrigger.ATTRIBUTE_UPDATED
                )
                is AttributeDeleteEvent -> handleEntityEvent(
                    tenantName,
                    setOf(entityEvent.attributeName),
                    Pair(entityEvent.getEntity(), entityEvent.getEntity()),
                    NotificationTrigger.ATTRIBUTE_DELETED
                )
            }
        }.onFailure {
            logger.warn("Entity event processing has failed: ${it.message}", it)
        }
    }

    private suspend fun handleEntityEvent(
        tenantName: String,
        updatedAttributes: Set<ExpandedTerm>,
        previousAndUpdatedPayloads: Pair<String, String>,
        notificationTrigger: NotificationTrigger
    ): Either<APIException, Disposable> = either {
        logger.debug("Attributes considered in the event: {}", updatedAttributes)
        val expandedEntityForMatching = ExpandedEntity(previousAndUpdatedPayloads.first.deserializeAsMap())
        val expandedEntityForNotification = ExpandedEntity(previousAndUpdatedPayloads.second.deserializeAsMap())
        mono {
            notificationService.notifyMatchingSubscribers(
                tenantName,
                Pair(expandedEntityForMatching, expandedEntityForNotification),
                updatedAttributes,
                notificationTrigger
            )
        }.contextWrite {
            it.put(NGSILD_TENANT_HEADER, tenantName)
        }.subscribe { notificationResult ->
            notificationResult.fold({
                if (it is OperationNotSupportedException)
                    logger.info(it.message)
                else
                    logger.error("Error when trying to notifiy subscribers: {}", it.message, it)
            }, { results ->
                val (succeeded, failed) = results.partition { it.third }.let { Pair(it.first.size, it.second.size) }
                logger.debug("Notified ${succeeded + failed} subscribers (success : $succeeded / failure : $failed)")
            })
        }
    }

    private fun String.getUpdatedAttributes() =
        this.deserializeAsMap()
            .keys
            .filter { !ignoredExpandedEntityProperties.contains(it) }
            .toSet()
}
