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
import com.egm.stellio.shared.util.AuthContextModel.IAM_TYPES
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.compactFragment
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.removeContextFromInput
import com.egm.stellio.shared.util.JsonUtils.serializeObject
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
                    kafkaTemplate.send(topic, event.entityId.toString(), serializeObject(event))
                    return true as java.lang.Boolean
                }
            )

    private fun entityChannelName(entityType: String) =
        if (IAM_TYPES.contains(entityType))
            "cim.iam.rights"
        else
            "cim.entity.$entityType"

    @Async
    fun publishEntityCreateEvent(
        entityId: URI,
        entityType: ExpandedTerm,
        contexts: List<String>
    ) {
        logger.debug("Sending create event for entity $entityId")
        val typeAndPayload = getSerializedEntity(entityId, contexts)
        publishEntityEvent(
            EntityCreateEvent(entityId, compactTerm(entityType, contexts), typeAndPayload.second, contexts)
        )
    }

    @Async
    fun publishEntityReplaceEvent(
        entityId: URI,
        entityType: ExpandedTerm,
        contexts: List<String>
    ) {
        logger.debug("Sending replace event for entity $entityId")
        val typeAndPayload = getSerializedEntity(entityId, contexts)
        publishEntityEvent(
            EntityReplaceEvent(entityId, compactTerm(entityType, contexts), typeAndPayload.second, contexts)
        )
    }

    @Async
    fun publishEntityDeleteEvent(
        entityId: URI,
        entityType: ExpandedTerm,
        contexts: List<String>
    ) {
        logger.debug("Sending delete event for entity $entityId")
        publishEntityEvent(
            EntityDeleteEvent(entityId, compactTerm(entityType, contexts), contexts)
        )
    }

    @Async
    fun publishAttributeAppendEvent(
        entityId: URI,
        entityType: String,
        attributeName: String,
        datasetId: URI? = null,
        overwrite: Boolean,
        operationPayload: String,
        updateOperationResult: UpdateOperationResult,
        contexts: List<String>
    ) {
        logger.debug("Sending append event for entity $entityId")
        val typeAndPayload = getSerializedEntity(entityId, contexts)
        if (updateOperationResult == UpdateOperationResult.APPENDED)
            publishEntityEvent(
                AttributeAppendEvent(
                    entityId,
                    entityType,
                    attributeName,
                    datasetId,
                    overwrite,
                    operationPayload,
                    typeAndPayload.second,
                    contexts
                )
            )
        else
            publishEntityEvent(
                AttributeReplaceEvent(
                    entityId,
                    entityType,
                    attributeName,
                    datasetId,
                    operationPayload,
                    typeAndPayload.second,
                    contexts
                )
            )
    }

    @Async
    fun publishAttributeAppendEvents(
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        appendResult: UpdateResult,
        contexts: List<String>
    ) {
        val typeAndPayload = getSerializedEntity(entityId, contexts)
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
                        compactTerm(typeAndPayload.first, contexts),
                        compactTerm(attributeName, contexts),
                        updatedDetails.datasetId,
                        true,
                        serializeObject(removeContextFromInput(compactFragment(attributePayload!!, contexts))),
                        typeAndPayload.second,
                        contexts
                    )
                )
            else
                publishEntityEvent(
                    AttributeReplaceEvent(
                        entityId,
                        compactTerm(typeAndPayload.first, contexts),
                        compactTerm(attributeName, contexts),
                        updatedDetails.datasetId,
                        serializeObject(removeContextFromInput(compactFragment(attributePayload!!, contexts))),
                        typeAndPayload.second,
                        contexts
                    )
                )
        }
    }

    @Async
    fun publishAttributeUpdateEvents(
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        updateResult: UpdateResult,
        contexts: List<String>
    ) {
        val typeAndPayload = getSerializedEntity(entityId, contexts)
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
                    compactTerm(typeAndPayload.first, contexts),
                    compactTerm(attributeName, contexts),
                    updatedDetails.datasetId,
                    serializeObject(removeContextFromInput(compactFragment(attributePayload!!, contexts))),
                    typeAndPayload.second,
                    contexts
                )
            )
        }
    }

    @Async
    fun publishPartialAttributeUpdateEvents(
        entityId: URI,
        jsonLdAttributes: Map<String, Any>,
        updatedDetails: List<UpdatedDetails>,
        contexts: List<String>
    ) {
        val typeAndPayload = getSerializedEntity(entityId, contexts)
        updatedDetails.forEach { updatedDetail ->
            val attributeName = updatedDetail.attributeName
            val attributePayload =
                JsonLdUtils.getAttributeFromExpandedAttributes(
                    jsonLdAttributes,
                    attributeName,
                    updatedDetail.datasetId
                )
            publishEntityEvent(
                AttributeUpdateEvent(
                    entityId,
                    compactTerm(typeAndPayload.first, contexts),
                    compactTerm(attributeName, contexts),
                    updatedDetail.datasetId,
                    serializeObject(removeContextFromInput(compactFragment(attributePayload!!, contexts))),
                    typeAndPayload.second,
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
        val typeAndPayload = getSerializedEntity(entityId, contexts)
        if (deleteAll)
            publishEntityEvent(
                AttributeDeleteAllInstancesEvent(
                    entityId,
                    compactTerm(typeAndPayload.first, contexts),
                    attributeName,
                    typeAndPayload.second,
                    contexts
                )
            )
        else
            publishEntityEvent(
                AttributeDeleteEvent(
                    entityId,
                    compactTerm(typeAndPayload.first, contexts),
                    attributeName,
                    datasetId,
                    typeAndPayload.second,
                    contexts
                )
            )
    }

    private fun getSerializedEntity(entityId: URI, contexts: List<String>): Pair<ExpandedTerm, String> =
        entityService.getFullEntityById(entityId, true)
            .let {
                Pair(it.type, compactAndSerialize(it, contexts, MediaType.APPLICATION_JSON))
            }
}
