package com.egm.stellio.entity.service

import arrow.core.Validated
import arrow.core.invalid
import arrow.core.valid
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeDeleteAllInstancesEvent
import com.egm.stellio.shared.model.AttributeDeleteEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityDeleteEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EntityReplaceEvent
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonUtils
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.internals.Topic
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import java.net.URI

@Component
class EntityEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val entityService: EntityService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    internal fun composeTopicName(entityType: String): Validated<Unit, String> {
        val topicName = entityChannelName(entityType)
        return try {
            Topic.validate(topicName)
            topicName.valid()
        } catch (e: InvalidTopicException) {
            logger.error("Invalid topic name generated for entity type $entityType: $topicName", e)
            return Unit.invalid()
        }
    }

    internal fun publishEntityEvent(event: EntityEvent): java.lang.Boolean =
        composeTopicName(event.entityType)
            .fold(
                {
                    false as java.lang.Boolean
                },
                { topic ->
                    kafkaTemplate.send(topic, event.entityId.toString(), JsonUtils.serializeObject(event))
                    return true as java.lang.Boolean
                }
            )

    private fun entityChannelName(channelSuffix: String) =
        "cim.entity.$channelSuffix"

    @Async
    fun publishEntityCreateEvent(
        entityId: URI,
        entityType: ExpandedTerm,
        entityPayload: String,
        contexts: List<String>
    ): Boolean {
        logger.debug("Sending create event for entity $entityId")
        return publishEntityEvent(
            EntityCreateEvent(entityId, compactTerm(entityType, contexts), entityPayload, contexts)
        ) as Boolean
    }

    @Async
    fun publishEntityReplaceEvent(
        entityId: URI,
        entityType: ExpandedTerm,
        entityPayload: String,
        contexts: List<String>
    ): Boolean {
        logger.debug("Sending replace event for entity $entityId")
        return publishEntityEvent(
            EntityReplaceEvent(entityId, compactTerm(entityType, contexts), entityPayload, contexts)
        ) as Boolean
    }

    @Async
    fun publishEntityDeleteEvent(
        entityId: URI,
        entityType: ExpandedTerm,
        contexts: List<String>
    ): Boolean {
        logger.debug("Sending delete event for entity $entityId")
        return publishEntityEvent(
            EntityDeleteEvent(entityId, compactTerm(entityType, contexts), contexts)
        ) as Boolean
    }

    @Async
    fun publishAttributeAppendEvent(
        observationEvent: AttributeAppendEvent,
        updateOperationResult: UpdateOperationResult
    ) {
        logger.debug("Sending append event for entity ${observationEvent.entityId}")
        val updatedEntity = entityService.getFullEntityById(observationEvent.entityId, true)
        if (updatedEntity!!.type.isEmpty()) {
            logger.warn("Retrieved entity ${observationEvent.entityId} but it has no type!")
            return
        }
        if (updateOperationResult == UpdateOperationResult.APPENDED)
            publishEntityEvent(
                AttributeAppendEvent(
                    observationEvent.entityId,
                    observationEvent.entityType,
                    observationEvent.attributeName,
                    observationEvent.datasetId,
                    observationEvent.overwrite,
                    observationEvent.operationPayload,
                    compactAndSerialize(
                        updatedEntity,
                        observationEvent.contexts,
                        MediaType.APPLICATION_JSON
                    ),
                    observationEvent.contexts
                )
            )
        else
            publishEntityEvent(
                AttributeReplaceEvent(
                    observationEvent.entityId,
                    observationEvent.entityType,
                    observationEvent.attributeName,
                    observationEvent.datasetId,
                    observationEvent.operationPayload,
                    compactAndSerialize(
                        updatedEntity,
                        observationEvent.contexts,
                        MediaType.APPLICATION_JSON
                    ),
                    observationEvent.contexts
                )
            )
    }

    @Async
    fun publishAppendEntityAttributesEvents(
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        appendResult: UpdateResult,
        contexts: List<String>
    ) {
        val updatedEntity = entityService.getFullEntityById(entityId, true)!!
        val serializedEntity = compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON)
        appendResult.updated.forEach { updatedDetails ->
            val attributeName = updatedDetails.attributeName
            val attributePayload =
                JsonLdUtils.getAttributeFromExpandedAttributes(
                    jsonLdAttributes,
                    attributeName,
                    updatedDetails.datasetId
                )
            if (updatedDetails.updateOperationResult == UpdateOperationResult.APPENDED)
                publishEntityEvent(
                    AttributeAppendEvent(
                        entityId,
                        compactTerm(updatedEntity.type, contexts),
                        compactTerm(attributeName, contexts),
                        updatedDetails.datasetId,
                        true,
                        JsonLdUtils.compactAndStringifyFragment(
                            attributeName,
                            attributePayload!!,
                            contexts
                        ),
                        serializedEntity,
                        contexts
                    )
                )
            else
                publishEntityEvent(
                    AttributeReplaceEvent(
                        entityId,
                        compactTerm(updatedEntity.type, contexts),
                        compactTerm(attributeName, contexts),
                        updatedDetails.datasetId,
                        JsonLdUtils.compactAndStringifyFragment(
                            attributeName,
                            attributePayload!!,
                            contexts
                        ),
                        serializedEntity,
                        contexts
                    )
                )
        }
    }

    @Async
    fun publishUpdateEntityAttributesEvents(
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        updateResult: UpdateResult,
        contexts: List<String>
    ) {
        val updatedEntity = entityService.getFullEntityById(entityId, true)!!
        val serializedEntity = compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON)
        updateResult.updated.forEach { updatedDetails ->
            val attributeName = updatedDetails.attributeName
            val attributePayload =
                JsonLdUtils.getAttributeFromExpandedAttributes(
                    jsonLdAttributes,
                    attributeName,
                    updatedDetails.datasetId
                )
            publishEntityEvent(
                AttributeReplaceEvent(
                    entityId,
                    compactTerm(updatedEntity.type, contexts),
                    compactTerm(attributeName, contexts),
                    updatedDetails.datasetId,
                    JsonLdUtils.compactAndStringifyFragment(
                        attributeName,
                        attributePayload!!,
                        contexts
                    ),
                    serializedEntity,
                    contexts
                )
            )
        }
    }

    @Async
    fun publishPartialUpdateEntityAttributesEvents(
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        updatedDetails: List<UpdatedDetails>,
        contexts: List<String>
    ) {
        val updatedEntity = entityService.getFullEntityById(entityId, true)!!
        val serializedEntity = compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON)
        updatedDetails.forEach { updatedDetail ->
            val attributeName = updatedDetail.attributeName
            val attributePayload =
                JsonLdUtils.getAttributeFromExpandedAttributes(
                    jsonLdAttributes,
                    attributeName,
                    updatedDetail.datasetId
                ) as Map<String, Any>
            publishEntityEvent(
                AttributeUpdateEvent(
                    entityId,
                    compactTerm(updatedEntity.type, contexts),
                    compactTerm(attributeName, contexts),
                    updatedDetail.datasetId,
                    JsonLdUtils.compactAndStringifyFragment(
                        attributePayload,
                        contexts
                    ),
                    serializedEntity,
                    contexts
                )
            )
        }
    }

    @Async
    fun publishAttributeDeleteEvent(
        entityId: URI,
        attributeName: String,
        datasetId: URI? = null,
        deleteAll: Boolean,
        contexts: List<String>
    ) {
        logger.debug("Sending delete event for attribute $attributeName of entity $entityId")
        val updatedEntity = entityService.getFullEntityById(entityId, true)!!
        if (deleteAll)
            publishEntityEvent(
                AttributeDeleteAllInstancesEvent(
                    entityId,
                    compactTerm(updatedEntity.type, contexts),
                    attributeName,
                    compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON),
                    contexts
                )
            )
        else
            publishEntityEvent(
                AttributeDeleteEvent(
                    entityId,
                    compactTerm(updatedEntity.type, contexts),
                    attributeName,
                    datasetId,
                    compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON),
                    contexts
                )
            )
    }
}
