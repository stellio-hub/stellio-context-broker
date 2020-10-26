package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.parseEntityEvent
import com.egm.stellio.shared.util.JsonUtils.parseNotification
import com.egm.stellio.shared.util.JsonUtils.parseSubscription
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
        when (val subscriptionEvent = parseEntityEvent(content)) {
            is EntityCreateEvent -> {
                val subscription = parseSubscription(subscriptionEvent.operationPayload)
                val entityTemporalProperty = TemporalEntityAttribute(
                    entityId = subscription.id,
                    type = "https://uri.etsi.org/ngsi-ld/Subscription",
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
                    entityPayload = subscriptionEvent.operationPayload
                )
                temporalEntityAttributeService.create(entityTemporalProperty)
                    .subscribe {
                        logger.debug("Created reference for subscription ${subscription.id}")
                    }
            }
            is EntityUpdateEvent -> logger.warn("Subscription update operation is not yet implemented")
            is EntityDeleteEvent -> logger.warn("Subscription delete operation is not yet implemented")
        }
    }

    @StreamListener("cim.notification")
    fun processNotification(content: String) {
        when (val notificationEvent = parseEntityEvent(content)) {
            is EntityCreateEvent -> {
                logger.debug("Received notification event payload: ${notificationEvent.operationPayload}")
                val notification = parseNotification(notificationEvent.operationPayload)
                val entitiesIds = mergeEntitesIdsFromNotificationData(notification.data)
                temporalEntityAttributeService.getFirstForEntity(notification.subscriptionId)
                    .flatMap {
                        val attributeInstance = AttributeInstance(
                            temporalEntityAttribute = it,
                            observedAt = notification.notifiedAt,
                            value = entitiesIds,
                            instanceId = notification.id
                        )
                        attributeInstanceService.create(attributeInstance)
                    }
                    .subscribe {
                        logger.debug("Created a new instance for notification ${notification.id}")
                    }
            }
            else -> logger.warn(
                "Received unexpected event type ${notificationEvent.operationType}" +
                    "for notification ${notificationEvent.entityId}"
            )
        }
    }

    fun mergeEntitesIdsFromNotificationData(data: List<Map<String, Any>>): String =
        data.joinToString(",") {
            it["id"] as String
        }
}
