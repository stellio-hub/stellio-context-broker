package com.egm.datahub.context.registry.service

import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class EntitiesListener {

    private val logger = LoggerFactory.getLogger(EntitiesListener::class.java)

    @KafkaListener(topics = ["entities"])
    fun processMessage(content: String) {
        logger.debug("Received entity $content")
        val model = Rio.parse(content.reader(), "", RDFFormat.JSONLD)
        logger.debug("Parsed entity into $model")
    }
}