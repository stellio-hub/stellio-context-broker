package com.egm.stellio.search.service

import arrow.core.Either
import com.egm.stellio.search.model.UpdateOperationResult
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.search.model.UpdatedDetails
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
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
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ): Job {
        val tenantUri = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug("Sending create event for entity {} in tenant {}", entityId, tenantUri)
            entity.onRight {
                publishEntityEvent(
                    EntityCreateEvent(sub, tenantUri, entityId, entityTypes, it.second, contexts)
                )
            }.logEntityEvent(EventsType.ENTITY_CREATE, entityId, tenantUri)
        }
    }

    suspend fun publishEntityReplaceEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ): Job {
        val tenantUri = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug("Sending replace event for entity {} in tenant {}", entityId, tenantUri)
            entity.onRight {
                publishEntityEvent(
                    EntityReplaceEvent(sub, tenantUri, entityId, entityTypes, it.second, contexts)
                )
            }.logEntityEvent(EventsType.ENTITY_REPLACE, entityId, tenantUri)
        }
    }

    suspend fun publishEntityDeleteEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ): Job {
        val tenantUri = getTenantFromContext()
        return coroutineScope.launch {
            logger.debug("Sending delete event for entity {} in tenant {}", entityId, tenantUri)
            publishEntityEvent(EntityDeleteEvent(sub, tenantUri, entityId, entityTypes, contexts))
        }
    }

    suspend fun publishAttributeChangeEvents(
        sub: String?,
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        updateResult: UpdateResult,
        overwrite: Boolean,
        contexts: List<String>
    ): Job {
        val tenantUri = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug("Sending attributes change events for entity {} in tenant {}", entityId, tenantUri)
            entity.onRight {
                updateResult.updated.forEach { updatedDetails ->
                    val attributeName = updatedDetails.attributeName
                    val serializedAttribute =
                        getSerializedAttribute(jsonLdAttributes, attributeName, updatedDetails.datasetId)
                    publishAttributeChangeEvent(
                        updatedDetails,
                        sub,
                        tenantUri,
                        entityId,
                        it,
                        serializedAttribute,
                        overwrite,
                        contexts
                    )
                }
            }.logAttributeEvent("Attribute Change", entityId, tenantUri)
        }
    }

    private fun publishAttributeChangeEvent(
        updatedDetails: UpdatedDetails,
        sub: String?,
        tenantUri: URI,
        entityId: URI,
        entityTypesAndPayload: Pair<List<ExpandedTerm>, String>,
        serializedAttribute: Pair<ExpandedTerm, String>,
        overwrite: Boolean,
        contexts: List<String>
    ) {
        when (updatedDetails.updateOperationResult) {
            UpdateOperationResult.APPENDED ->
                publishEntityEvent(
                    AttributeAppendEvent(
                        sub,
                        tenantUri,
                        entityId,
                        entityTypesAndPayload.first,
                        serializedAttribute.first,
                        updatedDetails.datasetId,
                        overwrite,
                        serializedAttribute.second,
                        entityTypesAndPayload.second,
                        contexts
                    )
                )

            UpdateOperationResult.REPLACED ->
                publishEntityEvent(
                    AttributeReplaceEvent(
                        sub,
                        tenantUri,
                        entityId,
                        entityTypesAndPayload.first,
                        serializedAttribute.first,
                        updatedDetails.datasetId,
                        serializedAttribute.second,
                        entityTypesAndPayload.second,
                        contexts
                    )
                )

            UpdateOperationResult.UPDATED ->
                publishEntityEvent(
                    AttributeUpdateEvent(
                        sub,
                        tenantUri,
                        entityId,
                        entityTypesAndPayload.first,
                        serializedAttribute.first,
                        updatedDetails.datasetId,
                        serializedAttribute.second,
                        entityTypesAndPayload.second,
                        contexts
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
        deleteAll: Boolean,
        contexts: List<String>
    ): Job {
        val tenantUri = getTenantFromContext()
        val entity = getSerializedEntity(entityId)
        return coroutineScope.launch {
            logger.debug(
                "Sending delete event for attribute {} of entity {} in tenant {}",
                attributeName,
                entityId,
                tenantUri
            )
            entity.onRight {
                if (deleteAll)
                    publishEntityEvent(
                        AttributeDeleteAllInstancesEvent(
                            sub,
                            tenantUri,
                            entityId,
                            it.first,
                            attributeName,
                            it.second,
                            contexts
                        )
                    )
                else
                    publishEntityEvent(
                        AttributeDeleteEvent(
                            sub,
                            tenantUri,
                            entityId,
                            it.first,
                            attributeName,
                            datasetId,
                            it.second,
                            contexts
                        )
                    )
            }.logAttributeEvent("Attribute Delete", entityId, tenantUri)
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
                val extractedPayload = getAttributeFromExpandedAttributes(jsonLdAttributes, attributeName, datasetId)!!
                Pair(attributeName, serializeObject(extractedPayload))
            }
        }

    private fun <A, B> Either<A, B>.logEntityEvent(eventsType: EventsType, entityId: URI, tenantUri: URI) =
        this.fold({
            logger.error("Error sending {} event for entity {} in tenant {}: {}", eventsType, entityId, tenantUri, it)
        }, {
            logger.debug("Sent {} event for entity {} in tenant {}", eventsType, entityId, tenantUri)
        })

    private fun <A, B> Either<A, B>.logAttributeEvent(eventCategory: String, entityId: URI, tenantUri: URI) =
        this.fold({
            logger.error(
                "Error sending {} event for entity {} in tenant {}: {}",
                eventCategory,
                entityId,
                tenantUri,
                it
            )
        }, {
            logger.debug("Sent {} event for entity {} in tenant {}", eventCategory, entityId, tenantUri)
        })
}
