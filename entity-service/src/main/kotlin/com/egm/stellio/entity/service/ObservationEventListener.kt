package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class ObservationEventListener(
    private val entityService: EntityService,
    private val entityAttributeService: EntityAttributeService,
    private val entityEventService: EntityEventService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    // using @KafkaListener instead of @StreamListener, couldn't find way to specify topic patterns with @StreamListener
    @KafkaListener(topicPattern = "cim.observation.*", groupId = "observations")
    fun processMessage(content: String) {
        when (val observationEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleEntityCreate(observationEvent)
            is AttributeUpdateEvent -> handleAttributeUpdateEvent(observationEvent)
            else -> logger.warn("Observation event ${observationEvent.operationType} not handled.")
        }
    }

    fun handleEntityCreate(observationEvent: EntityCreateEvent) {
        val ngsiLdEntity = JsonLdUtils.expandJsonLdEntity(
            observationEvent.operationPayload,
            observationEvent.contexts
        ).toNgsiLdEntity()

        try {
            entityService.createEntity(ngsiLdEntity)
        } catch (e: AlreadyExistsException) {
            logger.warn("Entity ${ngsiLdEntity.id} already exists: ${e.message}")
            return
        }

        entityEventService.publishEntityEvent(
            observationEvent,
            ngsiLdEntity.type
        )
    }

    fun handleAttributeUpdateEvent(observationEvent: AttributeUpdateEvent) {
        val expandedFragment = expandJsonLdFragment(
            observationEvent.operationPayload,
            observationEvent.contexts
        ) as Map<String, List<Any>>
        val expandedAttrId = expandJsonLdKey(observationEvent.attributeName, observationEvent.contexts)!!
        val expandedPayload = mapOf(expandedAttrId to expandedFragment)

        try {
            entityAttributeService.partialUpdateEntityAttribute(
                observationEvent.entityId,
                expandedPayload,
                observationEvent.contexts
            )
        } catch (e: ResourceNotFoundException) {
            logger.error("Entity or attribute not found in observation : ${e.message}")
            return
        }

        val updatedEntity = entityService.getFullEntityById(observationEvent.entityId, true)
        entityEventService.publishEntityEvent(
            AttributeUpdateEvent(
                observationEvent.entityId,
                observationEvent.attributeName,
                observationEvent.datasetId,
                observationEvent.operationPayload,
                compactAndSerialize(updatedEntity!!, observationEvent.contexts, MediaType.APPLICATION_JSON),
                observationEvent.contexts
            ),
            updatedEntity.type
        )
    }
}
