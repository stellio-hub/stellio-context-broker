package com.egm.datahub.context.registry.service

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntitiesListener {

    private val logger = LoggerFactory.getLogger(EntitiesListener::class.java)

    @KafkaListener(topics = ["entities"])
    fun processMessage(content: String) {
        logger.debug("Received entity $content")
        // TODO : to be implemented (in #698 for a first run)
    }
}