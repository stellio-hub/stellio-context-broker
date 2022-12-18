package com.egm.stellio.search.service

import arrow.core.Either
import com.egm.stellio.search.model.UpdateOperationResult
import com.egm.stellio.search.model.UpdateResult
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.compactFragment
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.compactTerms
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.JsonLdUtils.removeContextFromInput
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.internals.Topic
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.net.URI

@Component
class EntityEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val queryService: QueryService
) {

    private val catchAllTopic = "cim.entity._CatchAll"

    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    internal fun composeTopicName(entityType: String): String? {
        val topicName = entityChannelName(entityType)
        return try {
            Topic.validate(topicName)
            topicName
        } catch (e: InvalidTopicException) {
            val reason = "Invalid topic name generated for entity type $entityType: $topicName"
            logger.error(reason, e)
            null
        }
    }

    internal fun publishEntityEvent(event: EntityEvent): java.lang.Boolean =
        event.entityTypes
            .mapNotNull {
                composeTopicName(it)
            }.let { topics ->
                topics.forEach { topic ->
                    kafkaTemplate.send(topic, event.entityId.toString(), serializeObject(event))
                }
                kafkaTemplate.send(catchAllTopic, event.entityId.toString(), serializeObject(event))
                true as java.lang.Boolean
            }

    private fun entityChannelName(entityType: String): String =
        "cim.entity.$entityType"

    fun publishEntityCreateEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            logger.debug("Sending create event for entity $entityId")
            getSerializedEntity(entityId, contexts)
                .map {
                    publishEntityEvent(
                        EntityCreateEvent(sub, entityId, compactTerms(entityTypes, contexts), it.second, contexts)
                    )
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
            getSerializedEntity(entityId, contexts)
                .map {
                    publishEntityEvent(
                        EntityReplaceEvent(sub, entityId, compactTerms(entityTypes, contexts), it.second, contexts)
                    )
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
            publishEntityEvent(
                EntityDeleteEvent(sub, entityId, compactTerms(entityTypes, contexts), contexts)
            )
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
            getSerializedEntity(entityId, contexts)
                .map {
                    updateResult.updated.forEach { updatedDetails ->
                        val attributeName = updatedDetails.attributeName
                        val serializedAttribute =
                            getSerializedAttribute(jsonLdAttributes, attributeName, updatedDetails.datasetId, contexts)
                        when (updatedDetails.updateOperationResult) {
                            UpdateOperationResult.APPENDED ->
                                publishEntityEvent(
                                    AttributeAppendEvent(
                                        sub,
                                        entityId,
                                        compactTerms(it.first, contexts),
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
                                        compactTerms(it.first, contexts),
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
                                        compactTerms(it.first, contexts),
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
        attributeName: String,
        datasetId: URI? = null,
        deleteAll: Boolean,
        contexts: List<String>
    ): Job =
        coroutineScope.launch {
            logger.debug("Sending delete event for attribute $attributeName of entity $entityId")
            getSerializedEntity(entityId, contexts)
                .map {
                    if (deleteAll)
                        publishEntityEvent(
                            AttributeDeleteAllInstancesEvent(
                                sub,
                                entityId,
                                compactTerms(it.first, contexts),
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
                                compactTerms(it.first, contexts),
                                attributeName,
                                datasetId,
                                it.second,
                                contexts
                            )
                        )
                }
        }

    internal suspend fun getSerializedEntity(
        entityId: URI,
        contexts: List<String>
    ): Either<APIException, Pair<List<ExpandedTerm>, String>> =
        queryService.queryEntity(entityId, contexts)
            .map {
                Pair(it.types, compactAndSerialize(it, contexts, MediaType.APPLICATION_JSON))
            }

    private fun getSerializedAttribute(
        jsonLdAttributes: Map<String, Any>,
        attributeName: ExpandedTerm,
        datasetId: URI?,
        contexts: List<String>
    ): Pair<String, String> =
        when (attributeName) {
            JSONLD_TYPE ->
                Pair(
                    JSONLD_TYPE_TERM,
                    serializeObject(compactTerms(jsonLdAttributes[JSONLD_TYPE] as List<ExpandedTerm>, contexts))
                )
            else -> {
                val extractedPayload = JsonLdUtils.getAttributeFromExpandedAttributes(
                    jsonLdAttributes,
                    attributeName,
                    datasetId
                )!!
                Pair(
                    compactTerm(attributeName, contexts),
                    serializeObject(removeContextFromInput(compactFragment(extractedPayload, contexts)))
                )
            }
        }
}

private fun <A, B> Either<A, B>.logResults(eventsType: EventsType, entityId: URI) =
    this.fold({
        logger.error("Error while sending $eventsType event for entity $entityId: $it")
    }, {
        logger.debug("Successfully sent event $eventsType event for entity $entityId")
    })
