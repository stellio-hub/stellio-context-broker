package com.egm.stellio.search.listener

import arrow.core.Either
import arrow.core.left
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerms
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class ObservationEventListener(
    private val entityPayloadService: EntityPayloadService,
    private val entityEventService: EntityEventService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    @KafkaListener(topicPattern = "cim.observation.*", groupId = "observations")
    fun processMessage(content: String) {
        logger.debug("Received event: $content")
        coroutineScope.launch {
            dispatchObservationMessage(content)
        }
    }

    internal suspend fun dispatchObservationMessage(content: String) {
        kotlin.runCatching {
            val observationEvent = deserializeAs<EntityEvent>(content)
            when (observationEvent) {
                is EntityCreateEvent -> handleEntityCreate(observationEvent)
                is AttributeUpdateEvent -> handleAttributeUpdateEvent(observationEvent)
                is AttributeAppendEvent -> handleAttributeAppendEvent(observationEvent)
                else -> OperationNotSupportedException(unhandledOperationType(observationEvent.operationType)).left()
            }.fold({
                if (it is OperationNotSupportedException)
                    logger.info(it.message)
                else
                    logger.error(observationEvent.failedHandlingMessage(it))
            }, {
                logger.debug(observationEvent.successfulHandlingMessage())
            })
        }.onFailure {
            logger.warn("Observation event processing has failed: ${it.message}", it)
        }
    }

    suspend fun handleEntityCreate(observationEvent: EntityCreateEvent): Either<APIException, Unit> =
        entityPayloadService.createEntity(
            observationEvent.operationPayload,
            observationEvent.contexts,
            observationEvent.sub
        ).onRight {
            entityEventService.publishEntityCreateEvent(
                observationEvent.sub,
                observationEvent.entityId,
                expandJsonLdTerms(observationEvent.entityTypes, observationEvent.contexts),
                observationEvent.contexts
            )
        }

    suspend fun handleAttributeUpdateEvent(observationEvent: AttributeUpdateEvent): Either<APIException, Unit> {
        val expandedPayload = expandJsonLdFragment(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        return entityPayloadService.partialUpdateAttribute(
            observationEvent.entityId,
            expandedPayload,
            observationEvent.sub
        ).map {
            // there is only one result for an event, a success or a failure
            if (it.notUpdated.isNotEmpty()) {
                val notUpdatedDetails = it.notUpdated.first()
                logger.info(
                    "Nothing updated for attribute ${observationEvent.attributeName}" +
                        " in entity ${observationEvent.entityId}: ${notUpdatedDetails.reason}"
                )
            } else {
                entityEventService.publishAttributeChangeEvents(
                    observationEvent.sub,
                    observationEvent.entityId,
                    expandedPayload,
                    it,
                    false,
                    observationEvent.contexts
                )
            }
        }
    }

    suspend fun handleAttributeAppendEvent(observationEvent: AttributeAppendEvent): Either<APIException, Unit> {
        val expandedPayload = expandJsonLdFragment(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        val ngsiLdAttributes = parseToNgsiLdAttributes(expandedPayload)
        return entityPayloadService.appendAttributes(
            observationEvent.entityId,
            ngsiLdAttributes,
            expandedPayload,
            !observationEvent.overwrite,
            observationEvent.sub
        ).map {
            if (it.notUpdated.isNotEmpty()) {
                val notUpdatedDetails = it.notUpdated.first()
                logger.info(
                    "Nothing appended for attribute ${observationEvent.attributeName}" +
                        " in entity ${observationEvent.entityId}: ${notUpdatedDetails.reason}"
                )
            } else {
                entityEventService.publishAttributeChangeEvents(
                    observationEvent.sub,
                    observationEvent.entityId,
                    expandedPayload,
                    it,
                    observationEvent.overwrite,
                    observationEvent.contexts
                )
            }
        }
    }
}
