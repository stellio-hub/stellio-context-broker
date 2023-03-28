package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.Some
import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.AuthorizationService
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NOTIFICATION_ATTR_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedProperty
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedDateTime
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.addSubAttribute
import com.egm.stellio.shared.util.entityNotFoundMessage
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.time.ZoneOffset
import java.time.ZonedDateTime

@Component
class SubscriptionEventListenerService(
    private val entityPayloadService: EntityPayloadService,
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService,
    private val authorizationService: AuthorizationService
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
            attributeName = NGSILD_NOTIFICATION_ATTR_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.STRING,
            createdAt = subscription.createdAt,
            payload = Json.of("{}")
        )

        return either {
            entityPayloadService.createEntityPayload(
                subscription.id,
                listOf(NGSILD_SUBSCRIPTION_PROPERTY),
                subscription.createdAt,
                subscriptionCreateEvent.operationPayload,
                subscriptionCreateEvent.contexts
            ).bind()
            temporalEntityAttributeService.create(entityTemporalProperty).bind()
            subscriptionCreateEvent.sub?.let {
                authorizationService.createAdminRight(subscriptionCreateEvent.entityId, Some(it)).bind()
            }
        }
    }

    private suspend fun handleSubscriptionDeleteEvent(
        subscriptionDeleteEvent: EntityDeleteEvent
    ): Either<APIException, Unit> =
        either {
            entityPayloadService.deleteEntityPayload(subscriptionDeleteEvent.entityId).bind()
            authorizationService.removeRightsOnEntity(subscriptionDeleteEvent.entityId).bind()
        }

    private suspend fun handleNotificationCreateEvent(
        notificationCreateEvent: EntityCreateEvent
    ): Either<APIException, Unit> {
        logger.debug("Received notification event payload: ${notificationCreateEvent.operationPayload}")
        val notification = deserializeAs<Notification>(notificationCreateEvent.operationPayload)
        val entitiesIds = mergeEntitiesIdsFromNotificationData(notification.data)

        return either {
            val subscriptionId = notification.subscriptionId
            val tea =
                temporalEntityAttributeService.getForEntity(subscriptionId, emptySet())
                    .firstOrNull()
                    .right()
                    .bind() ?: ResourceNotFoundException(entityNotFoundMessage(subscriptionId.toString()))
                    .left()
                    .bind<TemporalEntityAttribute>()
            val payload = buildExpandedProperty(entitiesIds)
                .addSubAttribute(NGSILD_OBSERVED_AT_PROPERTY, buildNonReifiedDateTime(notification.notifiedAt))
                .first()
            attributeInstanceService.create(
                AttributeInstance(
                    temporalEntityAttribute = tea.id,
                    timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT,
                    time = notification.notifiedAt,
                    value = entitiesIds,
                    instanceId = notification.id,
                    payload = payload
                )
            ).bind()
            temporalEntityAttributeService.updateStatus(tea.id, ZonedDateTime.now(ZoneOffset.UTC), payload).bind()
        }
    }

    fun mergeEntitiesIdsFromNotificationData(data: List<Map<String, Any>>): String =
        data.joinToString(",") {
            it["id"] as String
        }
}
