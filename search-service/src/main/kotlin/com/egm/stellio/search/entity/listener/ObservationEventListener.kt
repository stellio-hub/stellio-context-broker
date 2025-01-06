package com.egm.stellio.search.entity.listener

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.search.entity.model.OperationStatus
import com.egm.stellio.search.entity.model.SucceededAttributeOperationResult
import com.egm.stellio.search.entity.service.EntityEventService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.EntityCreateEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.OperationNotSupportedException
import com.egm.stellio.shared.model.toExpandedAttributes
import com.egm.stellio.shared.model.toNgsiLdEntity
import com.egm.stellio.shared.model.unhandledOperationType
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerms
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
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

@Component
class ObservationEventListener(
    private val entityService: EntityService,
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
            val tenantName = observationEvent.tenantName
            logger.debug(
                "Handling event {} for entity {} in tenant {}",
                observationEvent.operationType,
                observationEvent.entityId,
                observationEvent.tenantName
            )
            when (observationEvent) {
                is EntityCreateEvent -> handleEntityCreate(tenantName, observationEvent)
                is AttributeUpdateEvent -> handleAttributeUpdateEvent(tenantName, observationEvent)
                is AttributeAppendEvent -> handleAttributeAppendEvent(tenantName, observationEvent)
                else -> OperationNotSupportedException(unhandledOperationType(observationEvent.operationType)).left()
            }
        }.onFailure {
            logger.warn("Observation event processing has failed: ${it.message}", it)
        }
    }

    suspend fun handleEntityCreate(
        tenantName: String,
        observationEvent: EntityCreateEvent
    ): Either<APIException, Unit> = either {
        val expandedEntity = expandJsonLdEntity(
            observationEvent.operationPayload,
            observationEvent.contexts
        )
        val ngsiLdEntity = expandedEntity.toNgsiLdEntity().bind()

        mono {
            entityService.createEntity(
                ngsiLdEntity,
                expandedEntity,
                observationEvent.sub
            ).map {
                entityEventService.publishEntityCreateEvent(
                    observationEvent.sub,
                    observationEvent.entityId,
                    expandJsonLdTerms(observationEvent.entityTypes, observationEvent.contexts)
                )
            }
        }.writeContextAndSubscribe(tenantName, observationEvent)
    }

    suspend fun handleAttributeUpdateEvent(
        tenantName: String,
        observationEvent: AttributeUpdateEvent
    ): Either<APIException, Unit> = either {
        val expandedAttribute = expandAttribute(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        mono {
            entityService.partialUpdateAttribute(
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
                        listOf(
                            SucceededAttributeOperationResult(
                                observationEvent.attributeName,
                                observationEvent.datasetId,
                                OperationStatus.UPDATED,
                                expandedAttribute.toExpandedAttributes()
                            )
                        )
                    )
                }
            }
        }.writeContextAndSubscribe(tenantName, observationEvent)
    }

    suspend fun handleAttributeAppendEvent(
        tenantName: String,
        observationEvent: AttributeAppendEvent
    ): Either<APIException, Unit> = either {
        val expandedAttribute = expandAttribute(
            observationEvent.attributeName,
            observationEvent.operationPayload,
            observationEvent.contexts
        )

        mono {
            entityService.appendAttributes(
                observationEvent.entityId,
                expandedAttribute.toExpandedAttributes(),
                false,
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
                        listOf(
                            SucceededAttributeOperationResult(
                                observationEvent.attributeName,
                                observationEvent.datasetId,
                                OperationStatus.APPENDED,
                                expandedAttribute.toExpandedAttributes()
                            )
                        )
                    )
                }
            }
        }.writeContextAndSubscribe(tenantName, observationEvent)
    }

    private fun Mono<Either<APIException, Job>>.writeContextAndSubscribe(
        tenantName: String,
        event: EntityEvent
    ) = this.contextWrite {
        it.put(NGSILD_TENANT_HEADER, tenantName)
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
