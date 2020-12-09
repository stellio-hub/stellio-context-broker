package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.parseEntityEvent
import com.egm.stellio.shared.util.RECEIVED_NON_PARSEABLE_ENTITY
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.egm.stellio.shared.util.toUri
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.net.URI
import java.time.ZonedDateTime

@Component
class EntityEventListenerService(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener, couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_search")
    fun processMessage(content: String) {
        when (val entityEvent = parseEntityEvent(content)) {
            is EntityCreateEvent -> handleEntityCreateEvent(entityEvent)
            is EntityDeleteEvent -> logger.warn("Entity delete operation is not yet implemented")
            is AttributeAppendEvent -> handleAttributeAppendEvent(entityEvent)
            is AttributeReplaceEvent -> handleAttributeReplaceEvent(entityEvent)
            is AttributeUpdateEvent -> handleAttributeUpdateEvent(entityEvent)
            is AttributeDeleteEvent -> logger.warn("Attribute delete operation is not yet implemented")
        }
    }

    private fun handleEntityCreateEvent(entityCreateEvent: EntityCreateEvent) =
        try {
            temporalEntityAttributeService.createEntityTemporalReferences(
                entityCreateEvent.operationPayload,
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

    private fun handleAttributeAppendEvent(attributeAppendEvent: AttributeAppendEvent) {
        val expandedAttributeName = expandJsonLdKey(attributeAppendEvent.attributeName, attributeAppendEvent.contexts)!!
        val operationPayloadNode = jacksonObjectMapper().readTree(attributeAppendEvent.operationPayload)
        val attributeNode = operationPayloadNode[attributeAppendEvent.attributeName]
            ?: operationPayloadNode[expandedAttributeName]

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
            attributeNode,
            attributeReplaceEvent.updatedEntity
        )
    }

    private fun handleAttributeUpdateEvent(attributeUpdateEvent: AttributeUpdateEvent) {
        val expandedAttributeName = expandJsonLdKey(attributeUpdateEvent.attributeName, attributeUpdateEvent.contexts)!!
        val attributeNode = jacksonObjectMapper().readTree(attributeUpdateEvent.operationPayload)

        handleAttributeUpdate(
            attributeUpdateEvent.entityId,
            expandedAttributeName,
            attributeNode,
            attributeUpdateEvent.updatedEntity
        )
    }

    private fun handleAttributeUpdate(
        entityId: URI,
        expandedAttributeName: String,
        attributeValuesNode: JsonNode,
        updatedEntity: String
    ) {
        // TODO add missing checks:
        //  - existence of temporal entity attribute
        //  - needs optimization (lot of JSON-LD parsing, ...)
        if (!attributeValuesNode.has("observedAt")) {
            logger.info(
                "Ignoring update event for " +
                    "${expandedAttributeName.extractShortTypeFromExpanded()}, it has no observedAt information"
            )
            return
        }

        val rawAttributeValue = attributeValuesNode["value"]
        val parsedAttributeValue =
            if (rawAttributeValue.isNumber)
                Pair(null, valueToDoubleOrNull(rawAttributeValue.asDouble()))
            else
                Pair(valueToStringOrNull(rawAttributeValue.asText()), null)
        val datasetId = attributeValuesNode["datasetId"]?.asText()

        temporalEntityAttributeService.getForEntityAndAttribute(
            entityId, expandedAttributeName, datasetId
        ).zipWhen {
            val attributeInstance = AttributeInstance(
                temporalEntityAttribute = it,
                observedAt = ZonedDateTime.parse(attributeValuesNode["observedAt"].asText()),
                value = parsedAttributeValue.first,
                measuredValue = parsedAttributeValue.second
            )
            attributeInstanceService.create(attributeInstance)
                .then(
                    temporalEntityAttributeService.updateEntityPayload(
                        entityId,
                        updatedEntity
                    )
                )
        }.doOnError {
            logger.error("Failed to persist new attribute instance, ignoring it", it)
        }.doOnNext {
            logger.debug("Created new attribute instance for temporal entity attribute (${it.t1})")
        }.subscribe()
    }

    fun handleAttributeAppend(
        entityId: URI,
        expandedAttributeName: String,
        attributeValuesNode: JsonNode,
        updatedEntity: String,
        contexts: List<String>
    ) {
        if (!attributeValuesNode.has("observedAt")) return

        val rawAttributeValue = attributeValuesNode["value"]
        val parsedAttributeValue =
            if (rawAttributeValue.isNumber)
                Pair(null, valueToDoubleOrNull(rawAttributeValue.asDouble()))
            else
                Pair(valueToStringOrNull(rawAttributeValue.asText()), null)

        val attributeValueType =
            if (parsedAttributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
            else TemporalEntityAttribute.AttributeValueType.ANY

        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = entityId,
            type = expandJsonLdEntity(updatedEntity).type,
            attributeName = expandedAttributeName,
            attributeValueType = attributeValueType,
            datasetId = attributeValuesNode["datasetId"]?.asText()?.toUri()
        )
        val attributeInstance = AttributeInstance(
            temporalEntityAttribute = temporalEntityAttribute.id,
            observedAt = ZonedDateTime.parse(attributeValuesNode["observedAt"].asText()),
            measuredValue = parsedAttributeValue.second,
            value = parsedAttributeValue.first
        )

        temporalEntityAttributeService.create(temporalEntityAttribute).zipWhen {
            attributeInstanceService.create(attributeInstance).then(
                temporalEntityAttributeService.updateEntityPayload(entityId, updatedEntity)
            )
        }
            .doOnError {
                logger.error("Failed to persist new temporal entity attribute, ignoring it", it)
            }.doOnNext {
                logger.debug("Created new temporal entity attribute with one attribute instance")
            }.subscribe()
    }
}
