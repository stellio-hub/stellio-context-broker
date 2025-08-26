package com.egm.stellio.search.entity.service

import arrow.core.Either
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AttributeCreateEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventsType
import com.egm.stellio.shared.model.ExpandedAttributeInstance
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.getTenantFromContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.net.URI

@Component
class EntityEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val entityQueryService: EntityQueryService
) {

    private val catchAllTopic = "cim.entity._CatchAll"

    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    internal fun publishEntityEvent(event: EntityEvent): Boolean {
        kafkaTemplate.send(catchAllTopic, event.entityId.toString(), serializeObject(event))
        return true
    }

    suspend fun publishEntityCreateEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>
    ): Job {
        val tenantName = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug("Sending create event for entity {} in tenant {}", entityId, tenantName)
            entity.onRight { (_, serializedEntity) ->
                publishEntityEvent(
                    EntityCreateEvent(sub, tenantName, entityId, entityTypes, serializedEntity)
                )
            }.logEntityEvent(EventsType.ENTITY_CREATE, entityId, tenantName)
        }
    }

    suspend fun publishEntityDeleteEvent(
        sub: String?,
        previousEntity: Entity,
        deletedEntityPayload: ExpandedEntity
    ): Job {
        val tenantName = getTenantFromContext()
        return coroutineScope.launch {
            logger.debug("Sending delete event for entity {} in tenant {}", previousEntity.entityId, tenantName)
            publishEntityEvent(
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
    }

    suspend fun publishAttributeChangeEvents(
        sub: String?,
        entityId: URI,
        originalEntity: ExpandedEntity,
        attributesOperationsResults: List<SucceededAttributeOperationResult>
    ): Job {
        val tenantName = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            entity.onRight {
                attributesOperationsResults.forEach { attributeOperationResult ->
                    publishAttributeChangeEvent(
                        sub,
                        tenantName,
                        entityId,
                        it,
                        originalEntity,
                        attributeOperationResult
                    )
                }
            }.logAttributeEvent("Attribute Change", entityId, tenantName)
        }
    }

    private fun publishAttributeChangeEvent(
        sub: String?,
        tenantName: String,
        entityId: URI,
        entityTypesAndPayload: Pair<List<ExpandedTerm>, String>,
        originalEntity: ExpandedEntity,
        attributeOperationResult: SucceededAttributeOperationResult
    ) {
        val attributeName = attributeOperationResult.attributeName
        val (types, payload) = entityTypesAndPayload
        logger.debug(
            "Sending {} event for attribute {} of entity {} in tenant {}",
            attributeOperationResult.operationStatus,
            attributeName,
            entityId,
            tenantName
        )
        when (attributeOperationResult.operationStatus) {
            OperationStatus.CREATED ->
                publishEntityEvent(
                    AttributeCreateEvent(
                        sub,
                        tenantName,
                        entityId,
                        types,
                        attributeOperationResult.attributeName,
                        attributeOperationResult.datasetId,
                        serializeObject(attributeOperationResult.newExpandedValue),
                        payload
                    )
                )

            OperationStatus.UPDATED ->
                publishEntityEvent(
                    AttributeUpdateEvent(
                        sub,
                        tenantName,
                        entityId,
                        types,
                        attributeOperationResult.attributeName,
                        attributeOperationResult.datasetId,
                        getAttributePayload(originalEntity, attributeName, attributeOperationResult.datasetId),
                        serializeObject(attributeOperationResult.newExpandedValue),
                        payload
                    )
                )

            OperationStatus.DELETED ->
                publishEntityEvent(
                    AttributeDeleteEvent(
                        sub,
                        tenantName,
                        entityId,
                        types,
                        attributeOperationResult.attributeName,
                        attributeOperationResult.datasetId,
                        getAttributePayload(originalEntity, attributeName, attributeOperationResult.datasetId),
                        injectDeletedAttribute(payload, attributeName, attributeOperationResult.newExpandedValue)
                    )
                )

            else ->
                logger.warn(
                    "Received an unexpected result (${attributeOperationResult.operationStatus} " +
                        "for entity $entityId and attribute ${attributeOperationResult.attributeName}"
                )
        }
    }

    suspend fun publishAttributeDeleteEvent(
        sub: String?,
        entityId: URI,
        originalEntity: ExpandedEntity,
        attributeOperationResult: SucceededAttributeOperationResult
    ): Job {
        val tenantName = getTenantFromContext()
        val attributeName = attributeOperationResult.attributeName
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug(
                "Sending delete event for attribute {} of entity {} in tenant {}",
                attributeName,
                entityId,
                tenantName
            )
            entity.onRight {
                publishEntityEvent(
                    AttributeDeleteEvent(
                        sub,
                        tenantName,
                        entityId,
                        it.first,
                        attributeName,
                        attributeOperationResult.datasetId,
                        getAttributePayload(originalEntity, attributeName, attributeOperationResult.datasetId),
                        injectDeletedAttribute(it.second, attributeName, attributeOperationResult.newExpandedValue)
                    )
                )
            }.logAttributeEvent("Attribute Delete", entityId, tenantName)
        }
    }

    suspend fun publishAttributeDeletesOnEntityDeleteEvent(
        sub: String?,
        entityId: URI,
        originalEntity: ExpandedEntity,
        deletedEntity: ExpandedEntity,
        deleteAttributesOperationsResults: List<SucceededAttributeOperationResult>
    ): Job {
        val tenantName = getTenantFromContext()
        return coroutineScope.launch {
            deleteAttributesOperationsResults.forEach { attributeDeleteEvent ->
                val attributeName = attributeDeleteEvent.attributeName
                logger.debug(
                    "Sending delete event for attribute {} of entity {} in tenant {}",
                    attributeName,
                    entityId,
                    tenantName
                )
                publishEntityEvent(
                    AttributeDeleteEvent(
                        sub,
                        tenantName,
                        entityId,
                        deletedEntity.types,
                        attributeName,
                        attributeDeleteEvent.datasetId,
                        getAttributePayload(originalEntity, attributeName, attributeDeleteEvent.datasetId),
                        injectDeletedAttribute(
                            serializeObject(deletedEntity.members),
                            attributeName,
                            attributeDeleteEvent.newExpandedValue
                        )
                    )
                )
            }
        }
    }

    internal suspend fun getSerializedEntity(
        entityId: URI
    ): Either<APIException, Pair<List<ExpandedTerm>, String>> =
        entityQueryService.retrieve(entityId)
            .map { Pair(it.types, it.payload.asString()) }

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

    private fun getAttributePayload(
        entity: ExpandedEntity,
        expandedAttributeName: ExpandedTerm,
        datasetId: URI?
    ): String =
        serializeObject(entity.getAttribute(expandedAttributeName, datasetId)!!)

    private fun <A, B> Either<A, B>.logEntityEvent(eventsType: EventsType, entityId: URI, tenantName: String) =
        this.fold({
            logger.error("Error sending {} event for entity {} in tenant {}: {}", eventsType, entityId, tenantName, it)
        }, {
            logger.debug("Sent {} event for entity {} in tenant {}", eventsType, entityId, tenantName)
        })

    private fun <A, B> Either<A, B>.logAttributeEvent(eventCategory: String, entityId: URI, tenantName: String) =
        this.fold({
            logger.error(
                "Error sending {} event for entity {} in tenant {}: {}",
                eventCategory,
                entityId,
                tenantName,
                it
            )
        }, {
            logger.debug("Sent {} event for entity {} in tenant {}", eventCategory, entityId, tenantName)
        })
}
