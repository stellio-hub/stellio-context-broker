package com.egm.stellio.search.entity.service

import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.shared.model.AttributeCreateEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.util.ErrorMessages.Kafka.EVENT_BROKER_UNAVAILABLE_MESSAGE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.getTenantFromContext
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.common.errors.AuthenticationException
import org.apache.kafka.common.errors.AuthorizationException
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.transaction.NoTransactionException
import org.springframework.transaction.reactive.TransactionSynchronization
import org.springframework.transaction.reactive.TransactionSynchronizationManager
import reactor.core.publisher.Mono
import java.net.URI
import kotlin.time.Duration.Companion.seconds

/**
 * Entity events are attached to the current transaction and only handed to Kafka once it has committed, so that no
 * event is published for a write that was rolled back. While Kafka is unavailable, the transaction is rolled back
 * instead of committing a write whose events would be lost.
 */
@Component
class EntityEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val kafkaHealthService: KafkaHealthService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    // a single consumer hands events to the producer in the order transactions committed; it runs on the IO
    // dispatcher because send() blocks for up to max.block.ms when the topic metadata is missing
    private val committedEvents = Channel<EntityEvent>(Channel.UNLIMITED)
    private val senderScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val sender = senderScope.launch {
        for (event in committedEvents) publishEntityEvent(event)
    }

    @PreDestroy
    fun stopSender() {
        committedEvents.close()
        runBlocking { withTimeoutOrNull(SHUTDOWN_DRAIN_TIMEOUT) { sender.join() } }
            ?: logger.error("Entity event sender not finished after {}, queued events are lost", SHUTDOWN_DRAIN_TIMEOUT)
        senderScope.cancel()
    }

    // the sender loop must survive any failure, otherwise every later event would be silently dropped
    @Suppress("TooGenericExceptionCaught")
    internal fun publishEntityEvent(event: EntityEvent) {
        try {
            kafkaTemplate.send(ENTITY_EVENTS_TOPIC, event.entityId.toString(), serializeObject(event))
                .whenComplete { _, exception -> exception?.let { onPublishFailure(event, it) } }
        } catch (exception: Exception) {
            onPublishFailure(event, exception)
        }
    }

    suspend fun publishEntityCreateEvent(
        sub: String?,
        expandedEntity: ExpandedEntity
    ) {
        val tenantName = getTenantFromContext()
        logger.debug("Sending create event for entity {} in tenant {}", expandedEntity.id, tenantName)
        publishAfterCommit(
            EntityCreateEvent(
                sub,
                tenantName,
                expandedEntity.id,
                expandedEntity.types,
                serializeObject(expandedEntity.members)
            )
        )
    }

    suspend fun publishEntityDeleteEvent(
        sub: String?,
        previousEntity: Entity,
        deletedEntityPayload: ExpandedEntity
    ) {
        val tenantName = getTenantFromContext()
        logger.debug("Sending delete event for entity {} in tenant {}", previousEntity.entityId, tenantName)
        publishAfterCommit(
            EntityDeleteEvent(
                sub,
                tenantName,
                previousEntity.entityId,
                previousEntity.types,
                previousEntity.payload.asString(),
                serializeObject(deletedEntityPayload.members)
            )
        )
    }

    suspend fun publishAttributeChangeEvents(
        sub: String?,
        originalEntity: ExpandedEntity,
        updatedEntity: ExpandedEntity,
        attributesOperationsResults: List<SucceededAttributeOperationResult>
    ) {
        val tenantName = getTenantFromContext()
        val events = attributesOperationsResults.mapNotNull { attributeOperationResult ->
            buildAttributeChangeEvent(sub, tenantName, originalEntity, updatedEntity, attributeOperationResult)
        }
        publishAfterCommit(events)
    }

    suspend fun publishAttributeDeleteEvent(
        sub: String?,
        originalEntity: ExpandedEntity,
        updatedEntity: ExpandedEntity,
        attributeOperationResult: SucceededAttributeOperationResult
    ) {
        val tenantName = getTenantFromContext()
        publishAfterCommit(
            buildAttributeDeleteEvent(sub, tenantName, originalEntity, updatedEntity, attributeOperationResult)
        )
    }

    suspend fun publishAttributeDeletesOnEntityDeleteEvent(
        sub: String?,
        originalEntity: ExpandedEntity,
        deletedEntity: ExpandedEntity,
        deleteAttributesOperationsResults: List<SucceededAttributeOperationResult>
    ) {
        val tenantName = getTenantFromContext()
        val events = deleteAttributesOperationsResults.map { attributeOperationResult ->
            buildAttributeDeleteEvent(sub, tenantName, originalEntity, deletedEntity, attributeOperationResult)
        }
        publishAfterCommit(events)
    }

    internal fun injectDeletedAttribute(
        entityPayload: String,
        attributeName: ExpandedTerm,
        attributeInstance: ExpandedAttributeInstance
    ): String {
        val entityPayload = entityPayload.deserializeAsMap().toMutableMap()
        entityPayload.merge(attributeName, listOf(attributeInstance)) { currentValue, newValue ->
            (currentValue as List<Any>).plus(newValue as List<Any>)
        }

        return serializeObject(entityPayload)
    }

    private suspend fun publishAfterCommit(event: EntityEvent) = publishAfterCommit(listOf(event))

    private suspend fun publishAfterCommit(events: List<EntityEvent>) {
        if (events.isEmpty()) return

        val synchronizationManager = currentSynchronizationManager()
        if (synchronizationManager == null) {
            logger.warn("No active transaction, publishing {} entity events right away", events.size)
            events.forEach { committedEvents.trySend(it) }
            return
        }

        val transactionEvents = synchronizationManager.getResource(this) as? TransactionEvents
            ?: TransactionEvents().also {
                synchronizationManager.bindResource(this, it)
                synchronizationManager.registerSynchronization(it)
            }
        transactionEvents.events.addAll(events)
    }

    private suspend fun currentSynchronizationManager(): TransactionSynchronizationManager? =
        TransactionSynchronizationManager.forCurrentTransaction()
            .filter { it.isSynchronizationActive }
            .onErrorResume(NoTransactionException::class.java) { Mono.empty() }
            .awaitSingleOrNull()

    private fun onPublishFailure(event: EntityEvent, exception: Throwable) {
        logger.error(
            "Unable to publish {} event for entity {} in tenant {}, the event is lost",
            event.operationType,
            event.entityId,
            event.tenantName,
            exception
        )
        if (exception.hasCause<AuthorizationException>() || exception.hasCause<AuthenticationException>())
            kafkaHealthService.markUnavailable("not allowed to publish entity events (${exception.message})")
    }

    private fun buildAttributeChangeEvent(
        sub: String?,
        tenantName: String,
        originalEntity: ExpandedEntity,
        updatedEntity: ExpandedEntity,
        attributeOperationResult: SucceededAttributeOperationResult
    ): EntityEvent? {
        val attributeName = attributeOperationResult.attributeName
        logger.debug(
            "Sending {} event for attribute {} of entity {} in tenant {}",
            attributeOperationResult.operationStatus,
            attributeName,
            updatedEntity.id,
            tenantName
        )
        return when (attributeOperationResult.operationStatus) {
            OperationStatus.CREATED ->
                AttributeCreateEvent(
                    sub,
                    tenantName,
                    updatedEntity.id,
                    updatedEntity.types,
                    attributeOperationResult.attributeName,
                    attributeOperationResult.datasetId,
                    serializeObject(attributeOperationResult.newExpandedValue),
                    serializeObject(updatedEntity.members)
                )

            OperationStatus.UPDATED ->
                AttributeUpdateEvent(
                    sub,
                    tenantName,
                    updatedEntity.id,
                    updatedEntity.types,
                    attributeOperationResult.attributeName,
                    attributeOperationResult.datasetId,
                    getAttributePayload(originalEntity, attributeName, attributeOperationResult.datasetId),
                    serializeObject(attributeOperationResult.newExpandedValue),
                    serializeObject(updatedEntity.members)
                )

            OperationStatus.DELETED ->
                buildAttributeDeleteEvent(sub, tenantName, originalEntity, updatedEntity, attributeOperationResult)

            else -> {
                logger.warn(
                    "Received an unexpected result (${attributeOperationResult.operationStatus} " +
                        "for entity ${updatedEntity.id} and attribute ${attributeOperationResult.attributeName}"
                )
                null
            }
        }
    }

    private fun buildAttributeDeleteEvent(
        sub: String?,
        tenantName: String,
        originalEntity: ExpandedEntity,
        updatedEntity: ExpandedEntity,
        attributeOperationResult: SucceededAttributeOperationResult
    ): AttributeDeleteEvent {
        val attributeName = attributeOperationResult.attributeName
        logger.debug(
            "Sending delete event for attribute {} of entity {} in tenant {}",
            attributeName,
            updatedEntity.id,
            tenantName
        )
        return AttributeDeleteEvent(
            sub,
            tenantName,
            updatedEntity.id,
            updatedEntity.types,
            attributeName,
            attributeOperationResult.datasetId,
            getAttributePayload(originalEntity, attributeName, attributeOperationResult.datasetId),
            injectDeletedAttribute(
                serializeObject(updatedEntity.members),
                attributeName,
                attributeOperationResult.newExpandedValue
            )
        )
    }

    private fun getAttributePayload(
        entity: ExpandedEntity,
        expandedAttributeName: ExpandedTerm,
        datasetId: URI?
    ): String =
        entity.getAttribute(expandedAttributeName, datasetId)?.let {
            serializeObject(it)
        } ?: run {
            logger.error("Unable to find attribute $expandedAttributeName ($datasetId) in entity ${entity.id}")
            ""
        }

    private inner class TransactionEvents : TransactionSynchronization {
        val events = mutableListOf<EntityEvent>()

        override fun beforeCommit(readOnly: Boolean): Mono<Void> =
            if (kafkaHealthService.isAvailable()) Mono.empty()
            else Mono.error(EventBrokerUnavailableException())

        override fun afterCommit(): Mono<Void> {
            events.forEach { committedEvents.trySend(it) }
            return Mono.empty()
        }
    }

    // a checked APIException raised before commit does not roll back: Spring commits anyway. The runtime wrapper
    // rolls back, and TransactionalEitherInterceptor turns it back into a Left(InternalErrorException)
    private class EventBrokerUnavailableException :
        RuntimeException(InternalErrorException(EVENT_BROKER_UNAVAILABLE_MESSAGE))

    companion object {
        const val ENTITY_EVENTS_TOPIC = "cim.entity._CatchAll"

        private val SHUTDOWN_DRAIN_TIMEOUT = 10.seconds
    }
}
