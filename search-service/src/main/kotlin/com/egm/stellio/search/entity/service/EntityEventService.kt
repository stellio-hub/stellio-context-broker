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
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.getTenantFromContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.net.URI

@Component
class EntityEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val catchAllTopic = "cim.entity._CatchAll"

    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    internal fun publishEntityEvent(event: EntityEvent): Boolean {
        kafkaTemplate.send(catchAllTopic, event.entityId.toString(), serializeObject(event))
        return true
    }

    suspend fun publishEntityCreateEvent(
        sub: String?,
        expandedEntity: ExpandedEntity
    ): Job {
        val tenantName = getTenantFromContext()
        return coroutineScope.launch {
            logger.debug("Sending create event for entity {} in tenant {}", expandedEntity.id, tenantName)
            publishEntityEvent(
                EntityCreateEvent(
                    sub,
                    tenantName,
                    expandedEntity.id,
                    expandedEntity.types,
                    serializeObject(expandedEntity.members)
                )
            )
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
        originalEntity: ExpandedEntity,
        updatedEntity: ExpandedEntity,
        attributesOperationsResults: List<SucceededAttributeOperationResult>
    ): Job {
        val tenantName = getTenantFromContext()
        return coroutineScope.launch {
            attributesOperationsResults.forEach { attributeOperationResult ->
                publishAttributeChangeEvent(
                    sub,
                    tenantName,
                    originalEntity,
                    updatedEntity,
                    attributeOperationResult
                )
            }
        }
    }

    private fun publishAttributeChangeEvent(
        sub: String?,
        tenantName: String,
        originalEntity: ExpandedEntity,
        updatedEntity: ExpandedEntity,
        attributeOperationResult: SucceededAttributeOperationResult
    ) {
        val attributeName = attributeOperationResult.attributeName
        logger.debug(
            "Sending {} event for attribute {} of entity {} in tenant {}",
            attributeOperationResult.operationStatus,
            attributeName,
            updatedEntity.id,
            tenantName
        )
        when (attributeOperationResult.operationStatus) {
            OperationStatus.CREATED ->
                publishEntityEvent(
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
                )

            OperationStatus.UPDATED ->
                publishEntityEvent(
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
                )

            OperationStatus.DELETED ->
                publishEntityEvent(
                    AttributeDeleteEvent(
                        sub,
                        tenantName,
                        updatedEntity.id,
                        updatedEntity.types,
                        attributeOperationResult.attributeName,
                        attributeOperationResult.datasetId,
                        getAttributePayload(originalEntity, attributeName, attributeOperationResult.datasetId),
                        injectDeletedAttribute(
                            serializeObject(updatedEntity.members),
                            attributeName,
                            attributeOperationResult.newExpandedValue
                        )
                    )
                )

            else ->
                logger.warn(
                    "Received an unexpected result (${attributeOperationResult.operationStatus} " +
                        "for entity ${updatedEntity.id} and attribute ${attributeOperationResult.attributeName}"
                )
        }
    }

    suspend fun publishAttributeDeleteEvent(
        sub: String?,
        originalEntity: ExpandedEntity,
        updatedEntity: ExpandedEntity,
        attributeOperationResult: SucceededAttributeOperationResult
    ): Job {
        val tenantName = getTenantFromContext()
        val attributeName = attributeOperationResult.attributeName
        return coroutineScope.launch {
            logger.debug(
                "Sending delete event for attribute {} of entity {} in tenant {}",
                attributeName,
                updatedEntity.id,
                tenantName
            )
            publishEntityEvent(
                AttributeDeleteEvent(
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
            )
        }
    }

    suspend fun publishAttributeDeletesOnEntityDeleteEvent(
        sub: String?,
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
                    deletedEntity.id,
                    tenantName
                )
                publishEntityEvent(
                    AttributeDeleteEvent(
                        sub,
                        tenantName,
                        deletedEntity.id,
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
        entity.getAttribute(expandedAttributeName, datasetId)?.let {
            serializeObject(it)
        } ?: run {
            logger.error("Unable to find attribute $expandedAttributeName ($datasetId) in entity ${entity.id}")
            ""
        }
}
