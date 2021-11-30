package com.egm.stellio.search.service

import arrow.core.Invalid
import arrow.core.Valid
import arrow.core.Validated
import arrow.core.invalid
import arrow.core.valid
import com.egm.stellio.search.model.AttributeInstance
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
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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
            )
                .subscribe {
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
        val expandedAttributeName = expandJsonLdKey(attributeAppendEvent.attributeName, attributeAppendEvent.contexts)!!
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)

        handleAttributeAppend(
            attributeAppendEvent.entityId,
            attributeAppendEvent.entityType,
            expandedAttributeName,
            operationPayloadNode,
            attributeAppendEvent.updatedEntity,
            attributeAppendEvent.contexts
        )
    }

    private fun handleAttributeReplaceEvent(attributeReplaceEvent: AttributeReplaceEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeReplaceEvent.operationPayload)

        handleAttributeUpdate(
            attributeReplaceEvent.entityId,
            attributeReplaceEvent.attributeName,
            attributeReplaceEvent.datasetId,
            operationPayloadNode,
            attributeReplaceEvent.updatedEntity,
            attributeReplaceEvent.contexts
        )
    }

    private fun handleAttributeUpdateEvent(attributeUpdateEvent: AttributeUpdateEvent) {
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeUpdateEvent.operationPayload)

        handleAttributeUpdate(
            attributeUpdateEvent.entityId,
            attributeUpdateEvent.attributeName,
            attributeUpdateEvent.datasetId,
            operationPayloadNode,
            attributeUpdateEvent.updatedEntity,
            attributeUpdateEvent.contexts
        )
    }

    private fun handleAttributeUpdate(
        entityId: URI,
        attributeName: String,
        datasetId: URI?,
        operationPayload: JsonNode,
        updatedEntity: String,
        contexts: List<String>
    ) {
        // return early to avoid extra processing if the attribute is not a temporal one
        if (!operationPayload.has("observedAt")) {
            logger.info("Ignoring update event for $operationPayload, it has no observedAt information")
            return
        }
        val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)
        val fullAttributeInstance = extractAttributeInstanceFromCompactedEntity(
            compactedJsonLdEntity,
            attributeName,
            datasetId
        )
        // Since ATTRIBUTE_UPDATE events payload may not contain the attribute type
        if (!operationPayload.has("type")) {
            (operationPayload as ObjectNode).put("type", fullAttributeInstance["type"] as String)
        }

        when (val extractedAttributeMetadata = toTemporalAttributeMetadata(operationPayload)) {
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
                    val attributeInstance = AttributeInstance(
                        temporalEntityAttribute = it,
                        observedAt = attributeMetadata.observedAt,
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

    fun handleAttributeAppend(
        entityId: URI,
        entityType: String,
        expandedAttributeName: String,
        operationPayload: JsonNode,
        updatedEntity: String,
        contexts: List<String>
    ) {
        // return early to avoid extra processing if the attribute is not a temporal one
        if (!operationPayload.has("observedAt")) {
            logger.info("Ignoring append event for $operationPayload, it has no observedAt information")
            return
        }

        when (val extractedAttributeMetadata = toTemporalAttributeMetadata(operationPayload)) {
            is Invalid -> {
                logger.info(extractedAttributeMetadata.value)
                return
            }
            is Valid -> {
                val attributeMetadata = extractedAttributeMetadata.value
                val compactedJsonLdEntity = addContextsToEntity(deserializeObject(updatedEntity), contexts)

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
                    observedAt = attributeMetadata.observedAt,
                    measuredValue = attributeMetadata.measuredValue,
                    value = attributeMetadata.value,
                    jsonNode = operationPayload
                )

                temporalEntityAttributeService.create(temporalEntityAttribute).zipWhen {
                    attributeInstanceService.create(attributeInstance).then(
                        temporalEntityAttributeService.updateEntityPayload(
                            entityId,
                            serializeObject(compactedJsonLdEntity)
                        )
                    )
                }
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

    internal fun toTemporalAttributeMetadata(operationPayload: JsonNode): Validated<String, AttributeMetadata> {
        val attributeTypeAsText = operationPayload["type"].asText()
        val attributeType = kotlin.runCatching {
            TemporalEntityAttribute.AttributeType.valueOf(attributeTypeAsText)
        }.getOrNull() ?: return "Unsupported attribute type: $attributeTypeAsText".invalid()
        val attributeValue = when (attributeType) {
            TemporalEntityAttribute.AttributeType.Relationship -> Pair(operationPayload["object"].asText(), null)
            TemporalEntityAttribute.AttributeType.Property -> {
                val rawAttributeValue = operationPayload["value"]
                when {
                    rawAttributeValue == null ->
                        return "Unable to get a value from attribute: $operationPayload".invalid()
                    rawAttributeValue.isNumber -> Pair(null, valueToDoubleOrNull(rawAttributeValue.asDouble()))
                    else -> Pair(valueToStringOrNull(rawAttributeValue.asText()), null)
                }
            }
        }
        val attributeValueType =
            if (attributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
            else TemporalEntityAttribute.AttributeValueType.ANY

        return AttributeMetadata(
            measuredValue = attributeValue.second,
            value = attributeValue.first,
            valueType = attributeValueType,
            datasetId = operationPayload["datasetId"]?.asText()?.toUri(),
            type = attributeType,
            observedAt = ZonedDateTime.parse(operationPayload["observedAt"].asText())
        ).valid()
    }
}
