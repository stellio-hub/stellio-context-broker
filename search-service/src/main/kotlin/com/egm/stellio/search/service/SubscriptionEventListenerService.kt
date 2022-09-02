package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.left
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.toNgsiLdFormat
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class SubscriptionEventListenerService(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService,
    private val entityAccessRightsService: EntityAccessRightsService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val coroutineScope = CoroutineScope(Dispatchers.Default)

    @KafkaListener(topics = ["cim.subscription"], groupId = "search_service_subscription")
    fun processSubscription(content: String) {
        logger.debug("Processing message: $content")
        coroutineScope.launch {
            dispatchSubscriptionMessage(content)
        }
    }

    internal suspend fun dispatchSubscriptionMessage(content: String) {
        val subscriptionEvent = deserializeAs<EntityEvent>(content)
        kotlin.runCatching {
            when (subscriptionEvent) {
                is EntityCreateEvent -> handleSubscriptionCreateEvent(subscriptionEvent)
                is EntityDeleteEvent -> handleSubscriptionDeleteEvent(subscriptionEvent)
                else ->
                    OperationNotSupportedException(unhandledOperationType(subscriptionEvent.operationType)).left()
            }.fold({
                if (it is OperationNotSupportedException)
                    logger.info(it.message)
                else
                    logger.error(subscriptionEvent.failedHandlingMessage(it))
            }, {
                logger.debug(subscriptionEvent.successfulHandlingMessage())
            })
        }.onFailure {
            logger.error(subscriptionEvent.failedHandlingMessage(it))
        }
    }

    @KafkaListener(topics = ["cim.notification"], groupId = "search_service_notification")
    fun processNotification(content: String) {
        coroutineScope.launch {
            logger.debug("Processing message: $content")
            coroutineScope.launch {
                dispatchNotificationMessage(content)
            }
        }
    }

    internal suspend fun dispatchNotificationMessage(content: String) {
        val notificationEvent = deserializeAs<EntityEvent>(content)
        kotlin.runCatching {
            when (notificationEvent) {
                is EntityCreateEvent -> handleNotificationCreateEvent(notificationEvent)
                else ->
                    OperationNotSupportedException(
                        unhandledOperationType(notificationEvent.operationType)
                    ).left()
            }.fold({
                if (it is OperationNotSupportedException)
                    logger.info(it.message)
                else
                    logger.error(notificationEvent.failedHandlingMessage(it))
            }, {
                logger.debug(notificationEvent.successfulHandlingMessage())
            })
        }.onFailure {
            logger.error(notificationEvent.failedHandlingMessage(it))
        }
    }

    private suspend fun handleSubscriptionCreateEvent(
        subscriptionCreateEvent: EntityCreateEvent
    ): Either<APIException, Unit> {
        val subscription = deserializeAs<Subscription>(subscriptionCreateEvent.operationPayload)
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = subscription.id,
            types = listOf("https://uri.etsi.org/ngsi-ld/Subscription"),
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )

        return either {
            temporalEntityAttributeService.create(entityTemporalProperty).bind()
            entityAccessRightsService.setAdminRoleOnEntity(
                subscriptionCreateEvent.sub,
                subscriptionCreateEvent.entityId
            ).bind()
        }
    }

    private suspend fun handleSubscriptionDeleteEvent(
        subscriptionDeleteEvent: EntityDeleteEvent
    ): Either<APIException, Unit> =
        either {
            temporalEntityAttributeService.deleteTemporalEntityReferences(subscriptionDeleteEvent.entityId).bind()
            entityAccessRightsService.removeRolesOnEntity(subscriptionDeleteEvent.entityId).bind()
        }

    private suspend fun handleNotificationCreateEvent(
        notificationCreateEvent: EntityCreateEvent
    ): Either<APIException, Unit> {
        logger.debug("Received notification event payload: ${notificationCreateEvent.operationPayload}")
        val notification = deserializeAs<Notification>(notificationCreateEvent.operationPayload)
        val entitiesIds = mergeEntitiesIdsFromNotificationData(notification.data)

        return either {
            val teaUuid = temporalEntityAttributeService.getFirstForEntity(notification.subscriptionId).bind()
            val attributeInstance = AttributeInstance(
                temporalEntityAttribute = teaUuid,
                timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT,
                time = notification.notifiedAt,
                value = entitiesIds,
                instanceId = notification.id,
                payload = mapOf(
                    "type" to "Notification",
                    "value" to entitiesIds,
                    "observedAt" to notification.notifiedAt.toNgsiLdFormat()
                )
            )
            attributeInstanceService.create(attributeInstance).bind()
        }
    }

    fun mergeEntitiesIdsFromNotificationData(data: List<Map<String, Any>>): String =
        data.joinToString(",") {
            it["id"] as String
        }
}
