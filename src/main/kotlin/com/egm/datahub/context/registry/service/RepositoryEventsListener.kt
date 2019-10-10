package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.EntityEvent
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import reactor.core.publisher.toMono

@Component
class RepositoryEventsListener(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = LoggerFactory.getLogger(RepositoryEventsListener::class.java)

    @Async
    @EventListener
    fun handleRepositoryEvent(entityEvent: EntityEvent) {
        kafkaTemplate.send(
            "cim.entity.${entityEvent.entityType}",
            entityEvent.entityUrn, entityEvent.payload
        )
        .completable()
        .toMono()
        .subscribe {
            logger.debug("Sent event $entityEvent to topic cim.entity.${entityEvent.entityType}")
        }
    }
}