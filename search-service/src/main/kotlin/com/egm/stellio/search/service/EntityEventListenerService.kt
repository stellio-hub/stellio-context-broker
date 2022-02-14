package com.egm.stellio.search.service

import arrow.core.*
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.addContextToElement
import com.egm.stellio.shared.util.JsonLdUtils.addContextsToEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.RECEIVED_NON_PARSEABLE_ENTITY
import com.egm.stellio.shared.util.extractAttributeInstanceFromCompactedEntity
import com.egm.stellio.shared.util.toUri
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.net.URI
import java.time.ZonedDateTime

@Component
class EntityEventListenerService(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_search")
    fun processMessage(content: String) {
        logger.debug("Processing message: $content")
        when (val entityEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleEntityCreateEvent(entityEvent)
            is EntityDeleteEvent -> handleEntityDeleteEvent(entityEvent)
            is AttributeAppendEvent -> handleAttributeAppendEvent(entityEvent)
            is AttributeReplaceEvent -> handleAttributeReplaceEvent(entityEvent)
            is AttributeUpdateEvent -> handleAttributeUpdateEvent(entityEvent)
            is AttributeDeleteEvent -> handleAttributeDeleteEvent(entityEvent)
            is AttributeDeleteAllInstancesEvent -> handleAttributeDeleteAllInstancesEvent(entityEvent)
        }
    }

    private fun handleEntityCreateEvent(entityCreateEvent: EntityCreateEvent) =
        try {
            val operationPayload = addContextToElement(entityCreateEvent.operationPayload, entityCreateEvent.contexts)
            temporalEntityAttributeService.createEntityTemporalReferences(
                operationPayload,
                entityCreateEvent.contexts
            ).subscribe {
                logger.debug("Bootstrapped entity (records created: $it)")
            }
        } catch (e: BadRequestDataException) {
            logger.error(RECEIVED_NON_PARSEABLE_ENTITY, e)
        } catch (e: InvalidRequestException) {
            logger.error(RECEIVED_NON_PARSEABLE_ENTITY, e)
        }

    private fun handleEntityDeleteEvent(entityDeleteEvent: EntityDeleteEvent) =
        temporalEntityAttributeService.deleteTemporalEntityReferences(
            entityDeleteEvent.entityId
        ).subscribe {
            logger.debug("Deleted entity ${entityDeleteEvent.entityId} (records deleted: $it)")
        }

    private fun handleAttributeDeleteEvent(attributeDeleteEvent: AttributeDeleteEvent) {
        val expandedAttributeName = expandJsonLdKey(attributeDeleteEvent.attributeName, attributeDeleteEvent.contexts)!!
        val compactedJsonLdEntity = addContextsToEntity(
            deserializeObject(attributeDeleteEvent.updatedEntity),
            attributeDeleteEvent.contexts
        )

        temporalEntityAttributeService.deleteTemporalAttributeReferences(
            attributeDeleteEvent.entityId,
            expandedAttributeName,
            attributeDeleteEvent.datasetId
        ).zipWith(
            temporalEntityAttributeService.updateEntityPayload(
                attributeDeleteEvent.entityId,
                serializeObject(compactedJsonLdEntity)
            )
        ).subscribe {
            logger.debug(
                "Deleted temporal attribute $expandedAttributeName with datasetId " +
                    "${attributeDeleteEvent.datasetId} from entity ${attributeDeleteEvent.entityId} " +
                    "(records deleted: ${it.t1})"
            )
        }
    }

    private fun handleAttributeDeleteAllInstancesEvent(
        attributeDeleteAllInstancesEvent: AttributeDeleteAllInstancesEvent
    ) {
        val expandedAttributeName = expandJsonLdKey(
            attributeDeleteAllInstancesEvent.attributeName, attributeDeleteAllInstancesEvent.contexts
        )!!
        val compactedJsonLdEntity = addContextsToEntity(
            deserializeObject(attributeDeleteAllInstancesEvent.updatedEntity),
            attributeDeleteAllInstancesEvent.contexts
        )

        temporalEntityAttributeService.deleteTemporalAttributeAllInstancesReferences(
            attributeDeleteAllInstancesEvent.entityId,
            expandedAttributeName
        ).zipWith(
            temporalEntityAttributeService.updateEntityPayload(
                attributeDeleteAllInstancesEvent.entityId,
                serializeObject(compactedJsonLdEntity)
            )
        ).subscribe {
            logger.debug(
                "Deleted all temporal attributes of $expandedAttributeName " +
                    "from entity ${attributeDeleteAllInstancesEvent.entityId} (records deleted: ${it.t1})"
            )
        }
    }

    private fun handleAttributeAppendEvent(attributeAppendEvent: AttributeAppendEvent) {
        handleAttributeAppend(
            attributeAppendEvent.entityId,
            attributeAppendEvent.entityType,
            attributeAppendEvent.attributeName,
            attributeAppendEvent.datasetId,
            attributeAppendEvent.updatedEntity,
            attributeAppendEvent.contexts
        )
    }

    private fun handleAttributeReplaceEvent(attributeReplaceEvent: AttributeReplaceEvent) {
        handleAttributeUpdate(
            attributeReplaceEvent.entityId,
            attributeReplaceEvent.attributeName,
            attributeReplaceEvent.datasetId,
            attributeReplaceEvent.updatedEntity,
            attributeReplaceEvent.contexts
        )
    }

    private fun handleAttributeUpdateEvent(attributeUpdateEvent: AttributeUpdateEvent) {
        handleAttributeUpdate(
            attributeUpdateEvent.entityId,
            attributeUpdateEvent.attributeName,
            attributeUpdateEvent.datasetId,
            attributeUpdateEvent.updatedEntity,
            attributeUpdateEvent.contexts
        )
    }

    private fun handleAttributeUpdate(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        updatedEntity: String,
        contexts: List<String>
    ) {
        val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)
        val fullAttributeInstance = extractAttributeInstanceFromCompactedEntity(
            compactedJsonLdEntity,
            attributeName,
            datasetId
        )

        when (val extractedAttributeMetadata = toTemporalAttributeMetadata(fullAttributeInstance)) {
            is Invalid -> {
                logger.info(extractedAttributeMetadata.value)
                return
            }
            is Valid -> {
                val attributeMetadata = extractedAttributeMetadata.value
                val expandedAttributeName = expandJsonLdKey(attributeName, contexts)!!
                temporalEntityAttributeService.getForEntityAndAttribute(
                    entityId, expandedAttributeName, attributeMetadata.datasetId
                ).zipWhen {
                    val timeAndProperty =
                        if (attributeMetadata.observedAt != null)
                            Pair(attributeMetadata.observedAt, TemporalProperty.OBSERVED_AT)
                        else if (attributeMetadata.modifiedAt != null)
                            Pair(attributeMetadata.modifiedAt, TemporalProperty.MODIFIED_AT)
                        // in case of an attribute replace
                        else Pair(attributeMetadata.createdAt, TemporalProperty.MODIFIED_AT)
                    val attributeInstance = AttributeInstance(
                        temporalEntityAttribute = it,
                        timeProperty = timeAndProperty.second,
                        time = timeAndProperty.first,
                        value = attributeMetadata.value,
                        measuredValue = attributeMetadata.measuredValue,
                        payload = fullAttributeInstance
                    )
                    attributeInstanceService.create(attributeInstance)
                }.zipWhen {
                    if (it.t2 != -1)
                        temporalEntityAttributeService.updateEntityPayload(
                            entityId,
                            serializeObject(compactedJsonLdEntity)
                        ).onErrorReturn(-1)
                    else Mono.just(-1)
                }.subscribe {
                    if (it.t1.t2 != -1 && it.t2 != -1)
                        logger.debug("Created new attribute instance of $expandedAttributeName for $entityId")
                    else
                        logger.warn(
                            "Failed to persist new attribute instance $expandedAttributeName " +
                                "for $entityId, ignoring it (insert results: ${it.t1.t2} / ${it.t2}))"
                        )
                }
            }
        }
    }

    private fun handleAttributeAppend(
        entityId: URI,
        entityType: String,
        attributeName: String,
        datasetId: URI?,
        updatedEntity: String,
        contexts: List<String>
    ) {
        val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)
        val fullAttributeInstance = extractAttributeInstanceFromCompactedEntity(
            compactedJsonLdEntity,
            attributeName,
            datasetId
        )

        when (val extractedAttributeMetadata = toTemporalAttributeMetadata(fullAttributeInstance)) {
            is Invalid -> {
                logger.info(extractedAttributeMetadata.value)
                return
            }
            is Valid -> {
                val attributeMetadata = extractedAttributeMetadata.value
                val expandedAttributeName = expandJsonLdKey(attributeName, contexts)!!

                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = entityId,
                    type = expandJsonLdKey(entityType, contexts)!!,
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
                    payload = fullAttributeInstance
                )

                val attributeObservedAtInstanceMono =
                    if (attributeMetadata.observedAt != null) {
                        val attributeObservedAtInstance = attributeInstance.copy(
                            time = attributeMetadata.observedAt,
                            timeProperty = TemporalProperty.OBSERVED_AT
                        )
                        attributeInstanceService.create(attributeObservedAtInstance)
                    } else Mono.just(1)

                temporalEntityAttributeService.create(temporalEntityAttribute)
                    .then(attributeInstanceService.create(attributeInstance))
                    .then(attributeObservedAtInstanceMono)
                    .then(
                        temporalEntityAttributeService.updateEntityPayload(
                            entityId, serializeObject(compactedJsonLdEntity)
                        )
                    )
                    .doOnError {
                        logger.error(
                            "Failed to persist new temporal entity attribute for $entityId " +
                                "with attribute instance $expandedAttributeName, ignoring it (${it.message})"
                        )
                    }.doOnNext {
                        logger.debug(
                            "Created new temporal entity attribute for $entityId " +
                                "with attribute instance $expandedAttributeName"
                        )
                    }.subscribe()
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
            TemporalEntityAttribute.AttributeType.Property -> {
                when (val rawAttributeValue = jsonLdEntity["value"]) {
                    null ->
                        return "Unable to get a value from attribute: $jsonLdEntity".invalid()
                    else -> Pair(valueToStringOrNull(rawAttributeValue), valueToDoubleOrNull(rawAttributeValue))
                }
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
