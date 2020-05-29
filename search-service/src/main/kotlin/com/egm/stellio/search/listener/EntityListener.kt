package com.egm.stellio.search.listener

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.shared.util.NgsiLdParsingUtils.expandJsonLdKey
import com.egm.stellio.shared.util.NgsiLdParsingUtils.parseEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils.parseEntityEvent
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.time.ZonedDateTime

@Component
class EntityListener(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener as I couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_search")
    fun processMessage(content: String) {
        val entityEvent = parseEntityEvent(content)
        when (entityEvent.operationType) {
            EventType.CREATE -> {
                try {
                    temporalEntityAttributeService.createEntityTemporalReferences(entityEvent.payload!!)
                        .subscribe {
                            logger.debug("Bootstrapped entity")
                        }
                } catch (e: Exception) {
                    logger.error("Received a non-parseable entity : $content", e)
                }
            }
            EventType.UPDATE -> {
                // TODO add missing checks:
                //  - existence of temporal entity attribute
                //  - only add an attribute instance if observedAt (?)
                //  - handle double vs string value
                //  - needs optimization (lot of JSON-LD parsing, ...)
                val rawParsedData = jacksonObjectMapper().readTree(entityEvent.payload!!)
                val rawEntity = parseEntity(entityEvent.updatedEntity!!)
                val attributeName = rawParsedData.fieldNames().next()
                val expandedAttributeName = expandJsonLdKey(rawParsedData.fieldNames().next(), rawEntity.contexts)!!

                temporalEntityAttributeService.getForEntityAndAttribute(entityEvent.entityId, expandedAttributeName)
                    .zipWhen {
                        val attributeInstance = AttributeInstance(
                            temporalEntityAttribute = it,
                            observedAt = ZonedDateTime.parse(rawParsedData[attributeName]["observedAt"].asText()),
                            measuredValue = rawParsedData[attributeName]["value"].asDouble()
                        )
                        attributeInstanceService.create(attributeInstance)
                    }
                    .doOnError {
                        logger.error("Failed to persist new attribute instance, ignoring it", it)
                    }
                    .doOnNext {
                        logger.debug("Created new attribute instance for temporal entity attribute (${it.t1})")
                    }
                    .subscribe()
            }
            EventType.APPEND -> logger.warn("Append operation is not yet implemented")
            EventType.DELETE -> logger.warn("Delete operation is not yet implemented")
        }
    }
}
