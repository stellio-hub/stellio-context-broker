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
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
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
    private val temporalEntityService: TemporalEntityService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_search")
    fun processMessage(content: String) {
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
        temporalEntityService.deleteTemporalEntityReferences(
            entityDeleteEvent.entityId
        ).subscribe {
            logger.debug("Deleted entity (records deleted: $it)")
        }

    private fun handleAttributeDeleteEvent(attributeDeleteEvent: AttributeDeleteEvent) {
        val expandedAttributeName = expandJsonLdKey(attributeDeleteEvent.attributeName, attributeDeleteEvent.contexts)!!
        val compactedJsonLdEntity = addContextsToEntity(
            JsonUtils.deserializeObject(attributeDeleteEvent.updatedEntity), attributeDeleteEvent.contexts
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
            logger.debug("Deleted temporal attribute (records deleted: ${it.t1})")
        }
    }

    private fun handleAttributeDeleteAllInstancesEvent(
        attributeDeleteAllInstancesEvent: AttributeDeleteAllInstancesEvent
    ) {
        val expandedAttributeName = expandJsonLdKey(
            attributeDeleteAllInstancesEvent.attributeName, attributeDeleteAllInstancesEvent.contexts
        )!!
        val compactedJsonLdEntity = addContextsToEntity(
            JsonUtils.deserializeObject(
                attributeDeleteAllInstancesEvent.updatedEntity
            ),
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
            logger.debug("Deleted temporal attributes (records deleted: ${it.t1})")
        }
    }

    private fun handleAttributeAppendEvent(attributeAppendEvent: AttributeAppendEvent) {
        val expandedAttributeName = expandJsonLdKey(attributeAppendEvent.attributeName, attributeAppendEvent.contexts)!!
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
        val attributeNode = operationPayloadNode[attributeAppendEvent.attributeName]
            ?: operationPayloadNode[expandedAttributeName]

        if (attributeNode == null) {
            logger.warn("Unable to extract values from $attributeAppendEvent")
            return
        }

        handleAttributeAppend(
            attributeAppendEvent.entityId,
            expandedAttributeName,
            attributeNode,
            attributeAppendEvent.updatedEntity,
            attributeAppendEvent.contexts
        )
    }

    private fun handleAttributeReplaceEvent(attributeReplaceEvent: AttributeReplaceEvent) {
        val expandedAttributeName = expandJsonLdKey(
            attributeReplaceEvent.attributeName,
            attributeReplaceEvent.contexts
        )!!
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeReplaceEvent.operationPayload)
        val attributeNode = operationPayloadNode[attributeReplaceEvent.attributeName]
            ?: operationPayloadNode[expandedAttributeName]

        handleAttributeUpdate(
            attributeReplaceEvent.entityId,
            expandedAttributeName,
            attributeReplaceEvent.datasetId,
            attributeNode,
            attributeReplaceEvent.updatedEntity,
            attributeReplaceEvent.contexts
        )
    }

    private fun handleAttributeUpdateEvent(attributeUpdateEvent: AttributeUpdateEvent) {
        val expandedAttributeName = expandJsonLdKey(attributeUpdateEvent.attributeName, attributeUpdateEvent.contexts)!!
        val attributeNode = jacksonObjectMapper().readTree(attributeUpdateEvent.operationPayload)

        handleAttributeUpdate(
            attributeUpdateEvent.entityId,
            expandedAttributeName,
            attributeUpdateEvent.datasetId,
            attributeNode,
            attributeUpdateEvent.updatedEntity,
            attributeUpdateEvent.contexts
        )
    }

    private fun handleAttributeUpdate(
        entityId: URI,
        expandedAttributeName: String,
        datasetId: URI?,
        attributeValuesNode: JsonNode,
        updatedEntity: String,
        contexts: List<String>
    ) {
        // TODO add missing checks:
        //  - existence of temporal entity attribute

        // return early to avoid extra processing if the attribute is not a temporal one
        if (!attributeValuesNode.has("observedAt")) {
            logger.info("Ignoring append event for $attributeValuesNode, it has no observedAt information")
            return
        }
        val compactedJsonLdEntity = addContextsToEntity(JsonUtils.deserializeObject(updatedEntity), contexts)
        val attributeInstancePayload = extractAttributeInstanceFromCompactedEntity(
            compactedJsonLdEntity,
            compactTerm(expandedAttributeName, contexts),
            datasetId
        )
        // Since ATTRIBUTE_UPDATE events payload may not contain the attribute type
        if (!attributeValuesNode.has("type")) {
            (attributeValuesNode as ObjectNode).put("type", attributeInstancePayload["type"] as String)
        }

        when (val extractedAttributeMetadata = toTemporalAttributeMetadata(attributeValuesNode)) {
            is Invalid -> {
                logger.info(extractedAttributeMetadata.e)
                return
            }
            is Valid -> {
                val attributeMetadata = extractedAttributeMetadata.a
                temporalEntityAttributeService.getForEntityAndAttribute(
                    entityId, expandedAttributeName, attributeMetadata.datasetId
                ).zipWhen {
                    val attributeInstance = AttributeInstance(
                        temporalEntityAttribute = it,
                        observedAt = attributeMetadata.observedAt,
                        value = attributeMetadata.value,
                        measuredValue = attributeMetadata.measuredValue,
                        payload = attributeInstancePayload
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
        expandedAttributeName: String,
        attributeValuesNode: JsonNode,
        updatedEntity: String,
        contexts: List<String>
    ) {
        when (val extractedAttributeMetadata = toTemporalAttributeMetadata(attributeValuesNode)) {
            is Invalid -> {
                logger.info(extractedAttributeMetadata.e)
                return
            }
            is Valid -> {
                val attributeMetadata = extractedAttributeMetadata.a
                val compactedJsonLdEntity = addContextsToEntity(JsonUtils.deserializeObject(updatedEntity), contexts)

                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = entityId,
                    type = expandJsonLdKey(compactedJsonLdEntity.getType(), contexts)!!,
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
                    payload =
                        extractAttributeInstanceFromCompactedEntity(
                            compactedJsonLdEntity,
                            compactTerm(expandedAttributeName, contexts),
                            attributeMetadata.datasetId
                        )
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

    internal fun toTemporalAttributeMetadata(jsonNode: JsonNode): Validated<String, AttributeMetadata> {
        val attributeTypeAsText = jsonNode["type"].asText()
        val attributeType = kotlin.runCatching {
            TemporalEntityAttribute.AttributeType.valueOf(attributeTypeAsText)
        }.getOrNull() ?: return "Unsupported attribute type: $attributeTypeAsText".invalid()
        val attributeValue = when (attributeType) {
            TemporalEntityAttribute.AttributeType.Relationship -> Pair(jsonNode["object"].asText(), null)
            TemporalEntityAttribute.AttributeType.Property -> {
                val rawAttributeValue = jsonNode["value"]
                when {
                    rawAttributeValue == null -> return "Unable to get a value from attribute: $jsonNode".invalid()
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
            datasetId = jsonNode["datasetId"]?.asText()?.toUri(),
            type = attributeType,
            observedAt = ZonedDateTime.parse(jsonNode["observedAt"].asText())
        ).valid()
    }
}
