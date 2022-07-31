package com.egm.stellio.search.service

import arrow.core.flatMap
import com.egm.stellio.search.authorization.EntityAccessRightsService
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.toNgsiLdFormat
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

    @KafkaListener(topics = ["cim.subscription"], groupId = "search_service_subscription")
    suspend fun processSubscription(content: String) {
        when (val subscriptionEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleSubscriptionCreateEvent(subscriptionEvent)
            is EntityUpdateEvent -> logger.warn("Subscription update operation is not yet implemented")
            is EntityDeleteEvent -> handleSubscriptionDeleteEvent(subscriptionEvent)
        }
    }

    @KafkaListener(topics = ["cim.notification"], groupId = "search_service_notification")
    suspend fun processNotification(content: String) {
        when (val notificationEvent = deserializeAs<EntityEvent>(content)) {
            is EntityCreateEvent -> handleNotificationCreateEvent(notificationEvent)
            else -> logger.warn(
                "Received unexpected event type ${notificationEvent.operationType}" +
                    "for notification ${notificationEvent.entityId}"
            )
        }
    }

    private suspend fun handleSubscriptionCreateEvent(subscriptionCreateEvent: EntityCreateEvent) {
        val subscription = deserializeAs<Subscription>(subscriptionCreateEvent.operationPayload)
        val entityTemporalProperty = TemporalEntityAttribute(
            entityId = subscription.id,
            types = listOf("https://uri.etsi.org/ngsi-ld/Subscription"),
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )

        temporalEntityAttributeService.create(entityTemporalProperty)
            .map {
                entityAccessRightsService.setAdminRoleOnEntity(
                    subscriptionCreateEvent.sub,
                    subscriptionCreateEvent.entityId
                )
            }.fold({
                logger.warn("Error while creating reference for subscription ${subscription.id} ($it)")
            }, {
                logger.debug("Created reference for subscription ${subscription.id}")
            })
    }

    private suspend fun handleSubscriptionDeleteEvent(subscriptionDeleteEvent: EntityDeleteEvent) {
        temporalEntityAttributeService.deleteTemporalEntityReferences(subscriptionDeleteEvent.entityId)
            .map {
                entityAccessRightsService.removeRolesOnEntity(subscriptionDeleteEvent.entityId)
            }.fold({
                logger.warn(
                    "Error while deleting reference for subscription ${subscriptionDeleteEvent.entityId} ($it)"
                )
            }, {
                logger.debug("Deleted subscription ${subscriptionDeleteEvent.entityId} (records deleted: $it)")
            })
    }

    private suspend fun handleNotificationCreateEvent(notificationCreateEvent: EntityCreateEvent) {
        logger.debug("Received notification event payload: ${notificationCreateEvent.operationPayload}")
        val notification = deserializeAs<Notification>(notificationCreateEvent.operationPayload)
        val entitiesIds = mergeEntitiesIdsFromNotificationData(notification.data)
        temporalEntityAttributeService.getFirstForEntity(notification.subscriptionId)
            .flatMap {
                val attributeInstance = AttributeInstance(
                    temporalEntityAttribute = it,
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
                attributeInstanceService.create(attributeInstance)
            }
            .fold({
                logger.error(
                    "Failed to persist new notification instance ${notification.id} " +
                        "for subscription ${notification.subscriptionId}, ignoring it (${it.message})"
                )
            }, {
                logger.debug("Created new notification instance ${notification.id} for ${notification.subscriptionId}")
            })
    }

    fun mergeEntitiesIdsFromNotificationData(data: List<Map<String, Any>>): String =
        data.joinToString(",") {
            it["id"] as String
        }
}
