package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.parseEntitiesEvent
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
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
        when (val entityEvent = parseEntitiesEvent(content)) {
            is EntityCreateEvent -> {
                try {
                    temporalEntityAttributeService.createEntityTemporalReferences(entityEvent.operationPayload)
                        .subscribe {
                            logger.debug("Bootstrapped entity (records created: $it)")
                        }
                } catch (e: BadRequestDataException) {
                    logger.error("Received a non-parseable entity", e)
                } catch (e: InvalidRequestException) {
                    logger.error("Received a non-parseable entity", e)
                }
            }
            is EntityDeleteEvent -> logger.warn("Entity delete operation is not yet implemented")
            is AttributeAppendEvent -> logger.warn("Attribute append operation is not yet implemented")
            is AttributeReplaceEvent -> {
                // TODO add missing checks:
                //  - existence of temporal entity attribute
                //  - needs optimization (lot of JSON-LD parsing, ...)
                val rawParsedData = jacksonObjectMapper().readTree(entityEvent.operationPayload)
                val rawEntity = try {
                    expandJsonLdEntity(entityEvent.updatedEntity)
                } catch (e: BadRequestDataException) {
                    logger.error("Received a non-parseable entity", e)
                    return
                } catch (e: InvalidRequestException) {
                    logger.error("Received a non-parseable entity", e)
                    return
                }
                val attributeName = rawParsedData.fieldNames().next()
                val attributeValuesNode = rawParsedData[attributeName]

                if (!attributeValuesNode.has("observedAt")) {
                    logger.info("Ignoring update event for $attributeName, it has no observedAt information")
                    return
                }

                val expandedAttributeName = expandJsonLdKey(attributeName, rawEntity.contexts)!!
                val rawAttributeValue = attributeValuesNode["value"]
                val parsedAttributeValue =
                    if (rawAttributeValue.isNumber)
                        Pair(null, valueToDoubleOrNull(rawAttributeValue.asDouble()))
                    else
                        Pair(valueToStringOrNull(rawAttributeValue.asText()), null)
                val datasetId = attributeValuesNode["datasetId"]?.asText()

                temporalEntityAttributeService.getForEntityAndAttribute(
                    entityEvent.entityId, expandedAttributeName, datasetId
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
                                entityEvent.entityId,
                                entityEvent.updatedEntity
                            )
                        )
                }.doOnError {
                    logger.error("Failed to persist new attribute instance, ignoring it", it)
                }.doOnNext {
                    logger.debug("Created new attribute instance for temporal entity attribute (${it.t1})")
                }.subscribe()
            }
            is AttributeUpdateEvent -> logger.warn("Attribute update operation is not yet implemented")
            is AttributeDeleteEvent -> logger.warn("Attribute delete operation is not yet implemented")
        }
    }
}
