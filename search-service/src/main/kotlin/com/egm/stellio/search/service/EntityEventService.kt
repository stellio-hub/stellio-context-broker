package com.egm.stellio.search.service

import arrow.core.Either
import com.egm.stellio.search.model.UpdateOperationResult
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.JsonUtils.serializeObject
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

    fun publishEntityCreateEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            logger.debug("Sending create event for entity $entityId")
            getSerializedEntity(entityId)
                .onRight {
                    publishEntityEvent(EntityCreateEvent(sub, entityId, entityTypes, it.second, contexts))
                }
                .logResults(EventsType.ENTITY_CREATE, entityId)
        }

    fun publishEntityReplaceEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            logger.debug("Sending replace event for entity $entityId")
            getSerializedEntity(entityId)
                .onRight {
                    publishEntityEvent(EntityReplaceEvent(sub, entityId, entityTypes, it.second, contexts))
                }
                .logResults(EventsType.ENTITY_REPLACE, entityId)
        }

    fun publishEntityDeleteEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            logger.debug("Sending delete event for entity $entityId")
            publishEntityEvent(EntityDeleteEvent(sub, entityId, entityTypes, contexts))
        }

    fun publishAttributeChangeEvents(
        sub: String?,
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        updateResult: UpdateResult,
        overwrite: Boolean,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            getSerializedEntity(entityId)
                .onRight {
                    updateResult.updated.forEach { updatedDetails ->
                        val attributeName = updatedDetails.attributeName
                        val serializedAttribute =
                            getSerializedAttribute(jsonLdAttributes, attributeName, updatedDetails.datasetId)
                        when (updatedDetails.updateOperationResult) {
                            UpdateOperationResult.APPENDED ->
                                publishEntityEvent(
                                    AttributeAppendEvent(
                                        sub,
                                        entityId,
                                        it.first,
                                        serializedAttribute.first,
                                        updatedDetails.datasetId,
                                        overwrite,
                                        serializedAttribute.second,
                                        it.second,
                                        contexts
                                    )
                                )
                            UpdateOperationResult.REPLACED ->
                                publishEntityEvent(
                                    AttributeReplaceEvent(
                                        sub,
                                        entityId,
                                        it.first,
                                        serializedAttribute.first,
                                        updatedDetails.datasetId,
                                        serializedAttribute.second,
                                        it.second,
                                        contexts
                                    )
                                )
                            UpdateOperationResult.UPDATED ->
                                publishEntityEvent(
                                    AttributeUpdateEvent(
                                        sub,
                                        entityId,
                                        it.first,
                                        serializedAttribute.first,
                                        updatedDetails.datasetId,
                                        serializedAttribute.second,
                                        it.second,
                                        contexts
                                    )
                                )
                            else ->
                                logger.warn(
                                    "Received an unexpected result (${updatedDetails.updateOperationResult} " +
                                        "for entity $entityId and attribte ${updatedDetails.attributeName}"
                                )
                        }
                    }
                }
        }

    fun publishAttributeDeleteEvent(
        sub: String?,
        entityId: URI,
        attributeName: ExpandedTerm,
        datasetId: URI? = null,
        deleteAll: Boolean,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            logger.debug("Sending delete event for attribute $attributeName of entity $entityId")
            getSerializedEntity(entityId)
                .onRight {
                    if (deleteAll)
                        publishEntityEvent(
                            AttributeDeleteAllInstancesEvent(
                                sub,
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
                                entityId,
                                it.first,
                                attributeName,
                                datasetId,
                                it.second,
                                contexts
                            )
                        )
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
}

private fun <A, B> Either<A, B>.logResults(eventsType: EventsType, entityId: URI) =
    this.fold({
        logger.error("Error while sending $eventsType event for entity $entityId: $it")
    }, {
        logger.debug("Successfully sent event $eventsType event for entity $entityId")
    })
