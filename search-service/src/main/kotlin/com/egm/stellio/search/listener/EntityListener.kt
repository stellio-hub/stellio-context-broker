package com.egm.stellio.search.listener

import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.util.NgsiLdParsingUtils
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntityListener(
    private val entityService: EntityService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener as I couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.entities.*", groupId = "context_search")
    fun processMessage(content: String) {
        try {
            val entity = NgsiLdParsingUtils.parseEntity(content)
            entityService.createEntityTemporalReferences(entity)
                .subscribe {
                    logger.debug("Bootstrapped entity")
                }
        } catch (e: Exception) {
            logger.error("Received a non-parseable entity : $content", e)
        }
    }
}