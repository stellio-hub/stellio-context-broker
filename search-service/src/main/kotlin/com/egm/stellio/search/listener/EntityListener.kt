package com.egm.stellio.search.listener

import com.egm.stellio.search.model.EventType
import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.ObservationService
import com.egm.stellio.search.util.NgsiLdParsingUtils
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntityListener(
    private val entityService: EntityService,
    private val observationService: ObservationService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener as I couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entity.*", groupId = "context_search")
    fun processMessage(content: String) {
        val entityEvent = NgsiLdParsingUtils.parseEntityEvent(content)
        when (entityEvent.operationType) {
            EventType.CREATE -> {
                try {
                    val entity = NgsiLdParsingUtils.parseEntity(entityEvent.payload!!)
                    entityService.createEntityTemporalReferences(entity)
                        .subscribe {
                            logger.debug("Bootstrapped entity")
                        }
                } catch (e: Exception) {
                    logger.error("Received a non-parseable entity : $content", e)
                }
            }
            EventType.UPDATE -> {
                val observation = NgsiLdParsingUtils.parseTemporalPropertyUpdate(entityEvent.payload!!)
                observation?.let {
                    logger.debug("Parsed observation: $observation")
                    observationService.create(observation)
                        .doOnError {
                            logger.error("Failed to persist observation, ignoring it", it)
                        }
                        .doOnNext {
                            logger.debug("Created observation ($it)")
                        }
                        .subscribe()
                }
            }
            EventType.APPEND -> logger.warn("Append operation is not yet implemented")
            EventType.DELETE -> logger.warn("Delete operation is not yet implemented")
        }
    }
}