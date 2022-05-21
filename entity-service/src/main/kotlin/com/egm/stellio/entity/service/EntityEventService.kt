package com.egm.stellio.entity.service

import arrow.core.ValidatedNel
import arrow.core.invalidNel
import arrow.core.traverse
import arrow.core.valid
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SAP
import com.egm.stellio.shared.util.AuthContextModel.IAM_COMPACTED_TYPES
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.compactFragment
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.compactTerms
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

    private val catchAllTopic = "cim.entity._CatchAll"

    private val logger = LoggerFactory.getLogger(javaClass)

    data class TopicNameError(val reason: String)

    internal fun composeTopicName(entityType: String, attributeName: String?): ValidatedNel<TopicNameError, String> {
        val topicName = entityChannelName(entityType, attributeName)
        return try {
            Topic.validate(topicName)
            topicName.valid()
        } catch (e: InvalidTopicException) {
            val reason = "Invalid topic name generated for entity type $entityType: $topicName"
            logger.error(reason, e)
            TopicNameError(reason).invalidNel()
        }
    }

    internal fun publishEntityEvent(event: EntityEvent): java.lang.Boolean =
        event.entityTypes
            .traverse {
                composeTopicName(it, event.getAttribute())
            }
            .fold(
                {
                    false as java.lang.Boolean
                },
                { topics ->
                    topics.forEach { topic ->
                        kafkaTemplate.send(topic, event.entityId.toString(), serializeObject(event))
                    }
                    kafkaTemplate.send(catchAllTopic, event.entityId.toString(), serializeObject(event))
                    return true as java.lang.Boolean
                }
            )

    private fun entityChannelName(entityType: String, attributeName: String?) =
        if (IAM_COMPACTED_TYPES.contains(entityType) || attributeName?.equals(AUTH_TERM_SAP) == true)
            "cim.iam.rights"
        else
            "cim.entity.$entityType"

    @Async
    fun publishEntityCreateEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ) {
        logger.debug("Sending create event for entity $entityId")
        val typeAndPayload = getSerializedEntity(entityId, contexts)
        publishEntityEvent(
            EntityCreateEvent(sub, entityId, compactTerms(entityTypes, contexts), typeAndPayload.second, contexts)
        )
    }

    @Async
    fun publishEntityReplaceEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ) {
        logger.debug("Sending replace event for entity $entityId")
        val typeAndPayload = getSerializedEntity(entityId, contexts)
        publishEntityEvent(
            EntityReplaceEvent(sub, entityId, compactTerms(entityTypes, contexts), typeAndPayload.second, contexts)
        )
    }

    @Async
    fun publishEntityDeleteEvent(
        sub: String?,
        entityId: URI,
        entityTypes: List<ExpandedTerm>,
        contexts: List<String>
    ) {
        logger.debug("Sending delete event for entity $entityId")
        publishEntityEvent(
            EntityDeleteEvent(sub, entityId, compactTerms(entityTypes, contexts), contexts)
        )
    }

    @Async
    fun publishAttributeAppendEvent(
        sub: String?,
        entityId: URI,
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
                    sub,
                    entityId,
                    compactTerms(typeAndPayload.first, contexts),
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
                    sub,
                    entityId,
                    compactTerms(typeAndPayload.first, contexts),
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
        sub: String?,
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
                        sub,
                        entityId,
                        compactTerms(typeAndPayload.first, contexts),
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
                        sub,
                        entityId,
                        compactTerms(typeAndPayload.first, contexts),
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
        sub: String?,
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
                    sub,
                    entityId,
                    compactTerms(typeAndPayload.first, contexts),
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
        sub: String?,
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
                    sub,
                    entityId,
                    compactTerms(typeAndPayload.first, contexts),
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
        sub: String?,
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
                    sub,
                    entityId,
                    compactTerms(typeAndPayload.first, contexts),
                    attributeName,
                    typeAndPayload.second,
                    contexts
                )
            )
        else
            publishEntityEvent(
                AttributeDeleteEvent(
                    sub,
                    entityId,
                    compactTerms(typeAndPayload.first, contexts),
                    attributeName,
                    datasetId,
                    typeAndPayload.second,
                    contexts
                )
            )
    }

    private fun getSerializedEntity(entityId: URI, contexts: List<String>): Pair<List<ExpandedTerm>, String> =
        entityService.getFullEntityById(entityId, true)
            .let {
                Pair(it.types, compactAndSerialize(it, contexts, MediaType.APPLICATION_JSON))
            }
}
