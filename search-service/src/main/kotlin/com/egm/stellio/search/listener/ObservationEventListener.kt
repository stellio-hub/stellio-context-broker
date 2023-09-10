package com.egm.stellio.search.listener

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.search.service.EntityEventService
import com.egm.stellio.search.service.EntityPayloadService
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerms
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.toExpandedAttributes
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.net.URI

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
            val tenantUri = observationEvent.tenantUri
            logger.debug(
                "Handling event {} for entity {} in tenant {}",
                observationEvent.operationType,
                observationEvent.entityId,
                observationEvent.tenantUri
            )
            when (observationEvent) {
                is EntityCreateEvent -> handleEntityCreate(tenantUri, observationEvent)
                is AttributeUpdateEvent -> handleAttributeUpdateEvent(tenantUri, observationEvent)
                is AttributeAppendEvent -> handleAttributeAppendEvent(tenantUri, observationEvent)
                else -> OperationNotSupportedException(unhandledOperationType(observationEvent.operationType)).left()
            }
        }.onFailure {
            logger.warn("Observation event processing has failed: ${it.message}", it)
        }
    }

    suspend fun handleEntityCreate(
        tenantUri: URI,
        observationEvent: EntityCreateEvent
    ): Either<APIException, Unit> = either {
        mono {
            entityPayloadService.createEntity(
                observationEvent.operationPayload,
                observationEvent.contexts,
                observationEvent.sub
            ).map {
                entityEventService.publishEntityCreateEvent(
                    observationEvent.sub,
                    observationEvent.entityId,
                    expandJsonLdTerms(observationEvent.entityTypes, observationEvent.contexts),
                    observationEvent.contexts
                )
            }
        }.writeContextAndSubscribe(tenantUri, observationEvent)
    }

    suspend fun handleAttributeUpdateEvent(
        tenantUri: URI,
        observationEvent: AttributeUpdateEvent
    ): Either<APIException, Unit> = either {
        val expandedAttribute = expandAttribute(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        mono {
            entityPayloadService.partialUpdateAttribute(
                observationEvent.entityId,
                expandedAttribute,
                observationEvent.sub
            ).map {
                // there is only one result for an event, a success or a failure
                if (it.notUpdated.isNotEmpty()) {
                    val notUpdatedDetails = it.notUpdated.first()
                    logger.info(
                        "Nothing updated for attribute ${observationEvent.attributeName}" +
                            " in entity ${observationEvent.entityId}: ${notUpdatedDetails.reason}"
                    )
                    Job()
                } else {
                    entityEventService.publishAttributeChangeEvents(
                        observationEvent.sub,
                        observationEvent.entityId,
                        expandedAttribute.toExpandedAttributes(),
                        it,
                        false,
                        observationEvent.contexts
                    )
                }
            }
        }.writeContextAndSubscribe(tenantUri, observationEvent)
    }

    suspend fun handleAttributeAppendEvent(
        tenantUri: URI,
        observationEvent: AttributeAppendEvent
    ): Either<APIException, Unit> = either {
        val expandedAttribute = expandAttribute(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        mono {
            entityPayloadService.appendAttributes(
                observationEvent.entityId,
                expandedAttribute.toExpandedAttributes(),
                !observationEvent.overwrite,
                observationEvent.sub
            ).map {
                if (it.notUpdated.isNotEmpty()) {
                    val notUpdatedDetails = it.notUpdated.first()
                    logger.info(
                        "Nothing appended for attribute ${observationEvent.attributeName}" +
                            " in entity ${observationEvent.entityId}: ${notUpdatedDetails.reason}"
                    )
                    Job()
                } else {
                    entityEventService.publishAttributeChangeEvents(
                        observationEvent.sub,
                        observationEvent.entityId,
                        expandedAttribute.toExpandedAttributes(),
                        it,
                        observationEvent.overwrite,
                        observationEvent.contexts
                    )
                }
            }
        }.writeContextAndSubscribe(tenantUri, observationEvent)
    }

    private fun Mono<Either<APIException, Job>>.writeContextAndSubscribe(
        tenantUri: URI,
        event: EntityEvent
    ) = this.contextWrite {
        it.put(NGSILD_TENANT_HEADER, tenantUri)
    }.subscribe { createResult ->
        createResult.fold({
            if (it is OperationNotSupportedException)
                logger.info(it.message)
            else
                logger.error(
                    "Error when performing {} operation on entity {}: {}",
                    event.operationType,
                    event.entityId,
                    it.message,
                    it
                )
        }, {
            logger.debug(event.successfulHandlingMessage())
        })
    }
}
