package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.toNgsiLdFormat
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(SubscriptionSink::class)
class SubscriptionEventListenerService(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @StreamListener("cim.subscription")
    fun processSubscription(content: String) {
        when (val subscriptionEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleSubscriptionCreateEvent(subscriptionEvent)
            is EntityUpdateEvent -> logger.warn("Subscription update operation is not yet implemented")
            is EntityDeleteEvent -> logger.warn("Subscription delete operation is not yet implemented")
        }
    }

    @StreamListener("cim.notification")
    fun processNotification(content: String) {
        when (val notificationEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleNotificationCreateEvent(notificationEvent)
            else -> logger.warn(
                "Received unexpected event type ${notificationEvent.operationType}" +
                    "for notification ${notificationEvent.entityId}"
            )
        }
    }

    private fun handleSubscriptionCreateEvent(subscriptionCreateEvent: EntityCreateEvent) {
        val subscription = deserializeAs<Subscription>(subscriptionCreateEvent.operationPayload)
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = subscription.id,
            type = "https://uri.etsi.org/ngsi-ld/Subscription",
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
            entityPayload = subscriptionCreateEvent.operationPayload
        )
        temporalEntityAttributeService.create(entityTemporalProperty)
            .subscribe {
                logger.debug("Created reference for subscription ${subscription.id}")
            }
    }

    private fun handleNotificationCreateEvent(notificationCreateEvent: EntityCreateEvent) {
        logger.debug("Received notification event payload: ${notificationCreateEvent.operationPayload}")
        val notification = deserializeAs<Notification>(notificationCreateEvent.operationPayload)
        val entitiesIds = mergeEntitiesIdsFromNotificationData(notification.data)
        temporalEntityAttributeService.getFirstForEntity(notification.subscriptionId)
            .flatMap {
                val attributeInstance = AttributeInstance(
                    temporalEntityAttribute = it,
                    observedAt = notification.notifiedAt,
                    value = entitiesIds,
                    instanceId = notification.id,
                    payload = mapOf(
                        "type" to "Notification",
                        "value" to entitiesIds,
                        "observedAt" to notification.notifiedAt.toNgsiLdFormat()
                    )
                )
                attributeInstanceService.create(attributeInstance)
            }
            .doOnError {
                logger.error(
                    "Failed to persist new notification instance ${notification.id} " +
                        "for subscription ${notification.subscriptionId}, ignoring it (${it.message})"
                )
            }
            .doOnNext {
                logger.debug("Created new notification instance ${notification.id} for ${notification.subscriptionId}")
            }
            .subscribe()
    }

    fun mergeEntitiesIdsFromNotificationData(data: List<Map<String, Any>>): String =
        data.joinToString(",") {
            it["id"] as String
        }
}
