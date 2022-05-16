package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class ObservationEventListener(
    private val entityService: EntityService,
    private val entityAttributeService: EntityAttributeService,
    private val entityEventService: EntityEventService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topicPattern = "cim.observation.*", groupId = "observations")
    fun processMessage(content: String) {
        logger.debug("Received event: $content")
        when (val observationEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleEntityCreate(observationEvent)
            is AttributeUpdateEvent -> handleAttributeUpdateEvent(observationEvent)
            is AttributeAppendEvent -> handleAttributeAppendEvent(observationEvent)
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

        entityEventService.publishEntityCreateEvent(
            observationEvent.sub,
            ngsiLdEntity.id,
            ngsiLdEntity.type,
            observationEvent.contexts
        )
    }

    fun handleAttributeUpdateEvent(observationEvent: AttributeUpdateEvent) {
        val expandedPayload = expandJsonLdFragment(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        try {
            val updateResult = entityAttributeService.partialUpdateEntityAttribute(
                observationEvent.entityId,
                expandedPayload,
                observationEvent.contexts
            )
            // TODO things could be more fine-grained (e.g., one instance updated and not the other one)
            //  so we should also check the notUpdated data and remove them from the propagated payload
            if (updateResult.updated.isEmpty()) {
                logger.info(
                    "Nothing has been updated for attribute ${observationEvent.attributeName}" +
                        " in entity ${observationEvent.entityId}, returning"
                )
                return
            }

            entityEventService.publishPartialAttributeUpdateEvents(
                observationEvent.sub,
                observationEvent.entityId,
                expandedPayload,
                updateResult.updated,
                observationEvent.contexts
            )
        } catch (e: ResourceNotFoundException) {
            logger.error("Entity or attribute not found in observation : ${e.message}")
            return
        }
    }

    fun handleAttributeAppendEvent(observationEvent: AttributeAppendEvent) {
        val expandedPayload = expandJsonLdFragment(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        try {
            val ngsiLdAttributes = parseToNgsiLdAttributes(expandedPayload)
            val updateResult = entityService.appendEntityAttributes(
                observationEvent.entityId,
                ngsiLdAttributes,
                !observationEvent.overwrite
            )
            if (updateResult.notUpdated.isNotEmpty()) {
                logger.warn("Attribute could not be appended: ${updateResult.notUpdated}")
                return
            }

            entityEventService.publishAttributeAppendEvent(
                observationEvent.sub,
                observationEvent.entityId,
                observationEvent.attributeName,
                observationEvent.datasetId,
                observationEvent.overwrite,
                observationEvent.operationPayload,
                updateResult.updated[0].updateOperationResult,
                observationEvent.contexts
            )
        } catch (e: BadRequestDataException) {
            logger.error(e.message)
            return
        }
    }
}
