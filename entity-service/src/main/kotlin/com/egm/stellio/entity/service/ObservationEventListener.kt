package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactAndSerialize
import com.egm.stellio.shared.util.JsonLdUtils.parseAndExpandAttributeFragment
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

        entityEventService.publishEntityEvent(
            observationEvent,
            ngsiLdEntity.type
        )
    }

    fun handleAttributeUpdateEvent(observationEvent: AttributeUpdateEvent) {
        val expandedPayload = parseAndExpandAttributeFragment(
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
        } catch (e: ResourceNotFoundException) {
            logger.error("Entity or attribute not found in observation : ${e.message}")
            return
        }

        val updatedEntity = entityService.getFullEntityById(observationEvent.entityId, true)
        if (updatedEntity == null)
            logger.warn("Unable to retrieve entity ${observationEvent.entityId} from DB, not sending to Kafka")
        else
            entityEventService.publishEntityEvent(
                AttributeUpdateEvent(
                    observationEvent.entityId,
                    observationEvent.attributeName,
                    observationEvent.datasetId,
                    observationEvent.operationPayload,
                    compactAndSerialize(updatedEntity, observationEvent.contexts, MediaType.APPLICATION_JSON),
                    observationEvent.contexts
                ),
                updatedEntity.type
            )
    }

    fun handleAttributeAppendEvent(observationEvent: AttributeAppendEvent) {
        val expandedPayload = parseAndExpandAttributeFragment(
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

            entityEventService.publishAttributeAppend(
                observationEvent, updateResult.updated[0].updateOperationResult
            )
        } catch (e: BadRequestDataException) {
            logger.error(e.message)
            return
        }
    }
}
