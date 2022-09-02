package com.egm.stellio.search.service

import arrow.core.*
import arrow.core.continuations.either
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.addContextToElement
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerms
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.extractAttributeInstanceFromCompactedEntity
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.net.URI
import java.time.ZonedDateTime

@Component
class EntityEventListenerService(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService,
    private val entityAccessRightsService: EntityAccessRightsService,
    private val entityPayloadService: EntityPayloadService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    @KafkaListener(topics = ["cim.entity._CatchAll"], groupId = "context_search")
    fun processMessage(content: String) {
        logger.debug("Processing message: $content")
        coroutineScope.launch {
            dispatchMessage(content)
        }
    }

    internal suspend fun dispatchMessage(content: String) {
        val entityEvent = deserializeAs<EntityEvent>(content)
        kotlin.runCatching {
            when (entityEvent) {
                is EntityCreateEvent -> handleEntityCreateEvent(entityEvent)
                is EntityReplaceEvent -> handleEntityReplaceEvent(entityEvent)
                is EntityDeleteEvent -> handleEntityDeleteEvent(entityEvent)
                is AttributeAppendEvent ->
                    when (entityEvent.attributeName) {
                        JSONLD_TYPE_TERM -> handleEntityTypeAppendEvent(entityEvent)
                        else -> handleAttributeAppendEvent(entityEvent)
                    }
                is AttributeReplaceEvent -> handleAttributeReplaceEvent(entityEvent)
                is AttributeUpdateEvent -> handleAttributeUpdateEvent(entityEvent)
                is AttributeDeleteEvent -> handleAttributeDeleteEvent(entityEvent)
                is AttributeDeleteAllInstancesEvent -> handleAttributeDeleteAllInstancesEvent(entityEvent)
                else ->
                    OperationNotSupportedException(unhandledOperationType(entityEvent.operationType)).left()
            }.fold({
                if (it is OperationNotSupportedException)
                    logger.info(it.message)
                else
                    logger.error(entityEvent.failedHandlingMessage(it))
            }, {
                logger.debug(entityEvent.successfulHandlingMessage())
            })
        }.onFailure {
            logger.error(entityEvent.failedHandlingMessage(it))
        }
    }

    private suspend fun handleEntityCreateEvent(entityCreateEvent: EntityCreateEvent): Either<APIException, Unit> {
        val operationPayload = addContextToElement(entityCreateEvent.operationPayload, entityCreateEvent.contexts)
        return either {
            temporalEntityAttributeService.createEntityTemporalReferences(
                operationPayload,
                entityCreateEvent.contexts,
                entityCreateEvent.sub
            ).bind()
            entityAccessRightsService.setAdminRoleOnEntity(
                entityCreateEvent.sub,
                entityCreateEvent.entityId
            ).bind()
        }
    }

    private suspend fun handleEntityReplaceEvent(entityReplaceEvent: EntityReplaceEvent): Either<APIException, Unit> {
        val operationPayload = addContextToElement(entityReplaceEvent.operationPayload, entityReplaceEvent.contexts)
        return either {
            temporalEntityAttributeService.deleteTemporalEntityReferences(entityReplaceEvent.entityId).bind()
            temporalEntityAttributeService.createEntityTemporalReferences(
                operationPayload,
                entityReplaceEvent.contexts,
                entityReplaceEvent.sub
            ).bind()
            entityAccessRightsService.setAdminRoleOnEntity(
                entityReplaceEvent.sub,
                entityReplaceEvent.entityId
            ).bind()
        }
    }

    private suspend fun handleEntityDeleteEvent(entityDeleteEvent: EntityDeleteEvent): Either<APIException, Unit> =
        either {
            temporalEntityAttributeService.deleteTemporalEntityReferences(entityDeleteEvent.entityId).bind()
            entityAccessRightsService.removeRolesOnEntity(entityDeleteEvent.entityId).bind()
        }

    private suspend fun handleAttributeDeleteEvent(
        attributeDeleteEvent: AttributeDeleteEvent
    ): Either<APIException, Unit> {
        val expandedAttributeName =
            expandJsonLdTerm(attributeDeleteEvent.attributeName, attributeDeleteEvent.contexts)
        val compactedJsonLdEntity = addContextsToEntity(
            deserializeObject(attributeDeleteEvent.updatedEntity),
            attributeDeleteEvent.contexts
        )

        return either {
            temporalEntityAttributeService.deleteTemporalAttributeReferences(
                attributeDeleteEvent.entityId,
                expandedAttributeName,
                attributeDeleteEvent.datasetId
            ).bind()
            entityPayloadService.upsertEntityPayload(
                attributeDeleteEvent.entityId,
                serializeObject(compactedJsonLdEntity)
            ).bind()
        }
    }

    private suspend fun handleAttributeDeleteAllInstancesEvent(
        attributeDeleteAllInstancesEvent: AttributeDeleteAllInstancesEvent
    ): Either<APIException, Unit> {
        val expandedAttributeName = expandJsonLdTerm(
            attributeDeleteAllInstancesEvent.attributeName, attributeDeleteAllInstancesEvent.contexts
        )
        val compactedJsonLdEntity = addContextsToEntity(
            deserializeObject(attributeDeleteAllInstancesEvent.updatedEntity),
            attributeDeleteAllInstancesEvent.contexts
        )

        return either {
            temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(
                attributeDeleteAllInstancesEvent.entityId,
                expandedAttributeName
            ).bind()
            entityPayloadService.upsertEntityPayload(
                attributeDeleteAllInstancesEvent.entityId,
                serializeObject(compactedJsonLdEntity)
            ).bind()
        }
    }

    private suspend fun handleEntityTypeAppendEvent(
        attributeAppendEvent: AttributeAppendEvent
    ): Either<APIException, Unit> {
        val (_, entityId, entityTypes, _, _, _, _, updatedEntity, contexts) = attributeAppendEvent
        val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)

        return either {
            temporalEntityAttributeService.updateTemporalEntityTypes(
                entityId,
                expandJsonLdTerms(entityTypes, contexts)
            ).bind()
            entityPayloadService.upsertEntityPayload(entityId, serializeObject(compactedJsonLdEntity)).bind()
        }
    }

    private suspend fun handleAttributeAppendEvent(
        attributeAppendEvent: AttributeAppendEvent
    ): Either<APIException, Unit> =
        handleAttributeAppend(attributeAppendEvent)

    private suspend fun handleAttributeReplaceEvent(
        attributeReplaceEvent: AttributeReplaceEvent
    ): Either<APIException, Unit> {
        val operationPayloadNode = mapper.readTree(attributeReplaceEvent.operationPayload)

        // FIXME since the NGSI-LD API still misses a proper API to partially update a list of attributes,
        //  replace events with an observedAt property are considered as observation updates, and not as audit ones
        return handleAttributeUpdate(
            attributeReplaceEvent.sub,
            attributeReplaceEvent.entityId,
            attributeReplaceEvent.entityTypes,
            attributeReplaceEvent.attributeName,
            attributeReplaceEvent.datasetId,
            operationPayloadNode.has("observedAt"),
            attributeReplaceEvent.updatedEntity,
            attributeReplaceEvent.contexts
        )
    }

    private suspend fun handleAttributeUpdateEvent(
        attributeUpdateEvent: AttributeUpdateEvent
    ): Either<APIException, Unit> {
        val operationPayloadNode = mapper.readTree(attributeUpdateEvent.operationPayload)

        return handleAttributeUpdate(
            attributeUpdateEvent.sub,
            attributeUpdateEvent.entityId,
            attributeUpdateEvent.entityTypes,
            attributeUpdateEvent.attributeName,
            attributeUpdateEvent.datasetId,
            operationPayloadNode.has("observedAt"),
            attributeUpdateEvent.updatedEntity,
            attributeUpdateEvent.contexts
        )
    }

    private suspend fun handleAttributeUpdate(
        sub: String?,
        entityId: URI,
        entityTypes: List<String>,
        attributeName: String,
        datasetId: URI?,
        isNewObservation: Boolean,
        updatedEntity: String,
        contexts: List<String>
    ): Either<APIException, Unit> {
        val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)
        val fullAttributeInstance = extractAttributeInstanceFromCompactedEntity(
            compactedJsonLdEntity,
            attributeName,
            datasetId
        )

        return either {
            when (val extractedAttributeMetadata = toTemporalAttributeMetadata(fullAttributeInstance)) {
                is Invalid -> {
                    logger.info(extractedAttributeMetadata.value)
                    extractedAttributeMetadata.value.right().bind()
                }
                is Valid -> {
                    val attributeMetadata = extractedAttributeMetadata.value
                    val expandedAttributeName = expandJsonLdTerm(attributeName, contexts)
                    val existingTeaUuid = temporalEntityAttributeService.getForEntityAndAttribute(
                        entityId, expandedAttributeName, attributeMetadata.datasetId
                    ).getOrHandle {
                        // in case of an existing attribute not handled previously by search service
                        // (not using observedAt), we transparently create the temporal entity attribute
                        val temporalEntityAttribute = TemporalEntityAttribute(
                            entityId = entityId,
                            types = expandJsonLdTerms(entityTypes, contexts),
                            attributeName = expandedAttributeName,
                            attributeType = attributeMetadata.type,
                            attributeValueType = attributeMetadata.valueType,
                            datasetId = attributeMetadata.datasetId
                        )
                        temporalEntityAttributeService.create(temporalEntityAttribute)
                            .map { temporalEntityAttribute.id }.bind()
                    }
                    val timeAndProperty =
                        if (isNewObservation)
                            Pair(attributeMetadata.observedAt!!, TemporalProperty.OBSERVED_AT)
                        else if (attributeMetadata.modifiedAt != null)
                            Pair(attributeMetadata.modifiedAt, TemporalProperty.MODIFIED_AT)
                        // in case of an attribute replace
                        else Pair(attributeMetadata.createdAt, TemporalProperty.MODIFIED_AT)
                    val attributeInstance = AttributeInstance(
                        temporalEntityAttribute = existingTeaUuid,
                        timeProperty = timeAndProperty.second,
                        time = timeAndProperty.first,
                        value = attributeMetadata.value,
                        measuredValue = attributeMetadata.measuredValue,
                        payload = fullAttributeInstance,
                        sub = sub
                    )
                    attributeInstanceService.create(attributeInstance).bind()
                    entityPayloadService.upsertEntityPayload(entityId, serializeObject(compactedJsonLdEntity)).bind()
                }
            }
        }
    }

    private suspend fun handleAttributeAppend(attributeAppendEvent: AttributeAppendEvent): Either<APIException, Unit> {
        val (sub, entityId, entityTypes, attributeName, datasetId, _, _, updatedEntity, contexts) = attributeAppendEvent
        val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)
        val fullAttributeInstance = extractAttributeInstanceFromCompactedEntity(
            compactedJsonLdEntity,
            attributeName,
            datasetId
        )

        return either {
            when (val extractedAttributeMetadata = toTemporalAttributeMetadata(fullAttributeInstance)) {
                is Invalid -> {
                    logger.info(extractedAttributeMetadata.value)
                    extractedAttributeMetadata.value.right().bind()
                }
                is Valid -> {
                    val attributeMetadata = extractedAttributeMetadata.value
                    val expandedAttributeName = expandJsonLdTerm(attributeName, contexts)

                    val temporalEntityAttribute = TemporalEntityAttribute(
                        entityId = entityId,
                        types = expandJsonLdTerms(entityTypes, contexts),
                        attributeName = expandedAttributeName,
                        attributeType = attributeMetadata.type,
                        attributeValueType = attributeMetadata.valueType,
                        datasetId = attributeMetadata.datasetId
                    )
                    val attributeInstance = AttributeInstance(
                        temporalEntityAttribute = temporalEntityAttribute.id,
                        timeProperty = TemporalProperty.CREATED_AT,
                        time = attributeMetadata.createdAt,
                        measuredValue = attributeMetadata.measuredValue,
                        value = attributeMetadata.value,
                        payload = fullAttributeInstance,
                        sub = sub
                    )

                    temporalEntityAttributeService.create(temporalEntityAttribute).bind()
                    attributeInstanceService.create(attributeInstance).bind()
                    if (attributeMetadata.observedAt != null) {
                        val attributeObservedAtInstance = attributeInstance.copy(
                            time = attributeMetadata.observedAt,
                            timeProperty = TemporalProperty.OBSERVED_AT
                        )
                        attributeInstanceService.create(attributeObservedAtInstance)
                    }

                    entityPayloadService.upsertEntityPayload(entityId, serializeObject(compactedJsonLdEntity)).bind()
                }
            }
        }
    }

    internal fun toTemporalAttributeMetadata(
        jsonLdEntity: CompactedJsonLdEntity
    ): Validated<String, AttributeMetadata> {
        val attributeTypeAsText = jsonLdEntity["type"] as String
        val attributeType = kotlin.runCatching {
            TemporalEntityAttribute.AttributeType.valueOf(attributeTypeAsText)
        }.getOrNull() ?: return "Unsupported attribute type: $attributeTypeAsText".invalid()
        val attributeValue = when (attributeType) {
            TemporalEntityAttribute.AttributeType.Relationship -> Pair(jsonLdEntity["object"] as String, null)
            TemporalEntityAttribute.AttributeType.Property ->
                when (val rawAttributeValue = jsonLdEntity["value"]) {
                    null ->
                        return "Unable to get a value from attribute: $jsonLdEntity".invalid()
                    else -> Pair(valueToStringOrNull(rawAttributeValue), valueToDoubleOrNull(rawAttributeValue))
                }
        }
        val attributeValueType =
            if (attributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
            else TemporalEntityAttribute.AttributeValueType.ANY

        val observedAt =
            if (jsonLdEntity["observedAt"] != null)
                ZonedDateTime.parse(jsonLdEntity["observedAt"] as String)
            else null

        val modifiedAt =
            if (jsonLdEntity["modifiedAt"] != null)
                ZonedDateTime.parse(jsonLdEntity["modifiedAt"] as String)
            else null

        return AttributeMetadata(
            measuredValue = attributeValue.second,
            value = attributeValue.first,
            valueType = attributeValueType,
            datasetId = (jsonLdEntity["datasetId"] as? String)?.toUri(),
            type = attributeType,
            createdAt = ZonedDateTime.parse(jsonLdEntity["createdAt"] as String),
            modifiedAt = modifiedAt,
            observedAt = observedAt
        ).valid()
    }
}
