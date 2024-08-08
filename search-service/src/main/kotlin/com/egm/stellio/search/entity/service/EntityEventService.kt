package com.egm.stellio.search.entity.service

import arrow.core.Either
import com.egm.stellio.search.entity.model.EntityPayload
import com.egm.stellio.search.entity.model.UpdateOperationResult
import com.egm.stellio.search.entity.model.UpdateResult
import com.egm.stellio.search.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
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
    private val entityPayloadService: EntityPayloadService
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
            entity.onRight {
                publishEntityEvent(
                    EntityCreateEvent(sub, tenantName, entityId, entityTypes, it.second, emptyList())
                )
            }.logEntityEvent(EventsType.ENTITY_CREATE, entityId, tenantName)
        }
    }

    suspend fun publishEntityReplaceEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>
    ): Job {
        val tenantName = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug("Sending replace event for entity {} in tenant {}", entityId, tenantName)
            entity.onRight {
                publishEntityEvent(
                    EntityReplaceEvent(sub, tenantName, entityId, entityTypes, it.second, emptyList())
                )
            }.logEntityEvent(EventsType.ENTITY_REPLACE, entityId, tenantName)
        }
    }

    suspend fun publishEntityDeleteEvent(
        sub: String?,
        entityPayload: EntityPayload
    ): Job {
        val tenantName = getTenantFromContext()
        return coroutineScope.launch {
            logger.debug("Sending delete event for entity {} in tenant {}", entityPayload.entityId, tenantName)
            publishEntityEvent(
                EntityDeleteEvent(
                    sub,
                    tenantName,
                    entityPayload.entityId,
                    entityPayload.types,
                    entityPayload.payload.asString(),
                    emptyList()
                )
            )
        }
    }

    suspend fun publishAttributeChangeEvents(
        sub: String?,
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        updateResult: UpdateResult,
        overwrite: Boolean
    ): Job {
        val tenantName = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug("Sending attributes change events for entity {} in tenant {}", entityId, tenantName)
            entity.onRight {
                updateResult.updated.forEach { updatedDetails ->
                    val attributeName = updatedDetails.attributeName
                    val serializedAttribute =
                        getSerializedAttribute(jsonLdAttributes, attributeName, updatedDetails.datasetId)
                    publishAttributeChangeEvent(
                        updatedDetails,
                        sub,
                        tenantName,
                        entityId,
                        it,
                        serializedAttribute,
                        overwrite
                    )
                }
            }.logAttributeEvent("Attribute Change", entityId, tenantName)
        }
    }

    private fun publishAttributeChangeEvent(
        updatedDetails: UpdatedDetails,
        sub: String?,
        tenantName: String,
        entityId: URI,
        entityTypesAndPayload: Pair<List<ExpandedTerm>, String>,
        serializedAttribute: Pair<ExpandedTerm, String>,
        overwrite: Boolean
    ) {
        when (updatedDetails.updateOperationResult) {
            UpdateOperationResult.APPENDED ->
                publishEntityEvent(
                    AttributeAppendEvent(
                        sub,
                        tenantName,
                        entityId,
                        entityTypesAndPayload.first,
                        serializedAttribute.first,
                        updatedDetails.datasetId,
                        overwrite,
                        serializedAttribute.second,
                        entityTypesAndPayload.second,
                        emptyList()
                    )
                )

            UpdateOperationResult.REPLACED ->
                publishEntityEvent(
                    AttributeReplaceEvent(
                        sub,
                        tenantName,
                        entityId,
                        entityTypesAndPayload.first,
                        serializedAttribute.first,
                        updatedDetails.datasetId,
                        serializedAttribute.second,
                        entityTypesAndPayload.second,
                        emptyList()
                    )
                )

            UpdateOperationResult.UPDATED ->
                publishEntityEvent(
                    AttributeUpdateEvent(
                        sub,
                        tenantName,
                        entityId,
                        entityTypesAndPayload.first,
                        serializedAttribute.first,
                        updatedDetails.datasetId,
                        serializedAttribute.second,
                        entityTypesAndPayload.second,
                        emptyList()
                    )
                )

            else ->
                logger.warn(
                    "Received an unexpected result (${updatedDetails.updateOperationResult} " +
                        "for entity $entityId and attribute ${updatedDetails.attributeName}"
                )
        }
    }

    suspend fun publishAttributeDeleteEvent(
        sub: String?,
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI? = null,
        deleteAll: Boolean
    ): Job {
        val tenantName = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug(
                "Sending delete event for attribute {} of entity {} in tenant {}",
                attributeName,
                entityId,
                tenantName
            )
            entity.onRight {
                if (deleteAll)
                    publishEntityEvent(
                        AttributeDeleteAllInstancesEvent(
                            sub,
                            tenantName,
                            entityId,
                            it.first,
                            attributeName,
                            it.second,
                            emptyList()
                        )
                    )
                else
                    publishEntityEvent(
                        AttributeDeleteEvent(
                            sub,
                            tenantName,
                            entityId,
                            it.first,
                            attributeName,
                            datasetId,
                            it.second,
                            emptyList()
                        )
                    )
            }.logAttributeEvent("Attribute Delete", entityId, tenantName)
        }
    }

    internal suspend fun getSerializedEntity(
        entityId: URI
    ): Either<APIException, Pair<List<ExpandedTerm>, String>> =
        entityPayloadService.retrieve(entityId)
            .map {
                Pair(it.types, it.payload.asString())
            }

    private fun getSerializedAttribute(
        jsonLdAttributes: Map<String, Any>,
        attributeName: ExpandedTerm,
        datasetId: URI?
    ): Pair<ExpandedTerm, String> =
        when (attributeName) {
            JSONLD_TYPE -> Pair(JSONLD_TYPE, serializeObject(jsonLdAttributes[JSONLD_TYPE]!!))
            else -> {
                val extractedPayload = (jsonLdAttributes as ExpandedAttributes).getAttributeFromExpandedAttributes(
                    attributeName,
                    datasetId
                )!!
                Pair(attributeName, serializeObject(extractedPayload))
            }
        }

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
