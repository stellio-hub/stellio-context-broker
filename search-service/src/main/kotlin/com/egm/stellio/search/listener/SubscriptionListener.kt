package com.egm.stellio.search.listener

import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.util.ApiUtils.parseNotification
import com.egm.stellio.shared.util.ApiUtils.parseSubscription
import com.egm.stellio.shared.util.NgsiLdParsingUtils.parseEntityEvent
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(SubscriptionSink::class)
class SubscriptionListener(
    private val temporalEntityAttributeService: TemporalEntityAttributeService,
    private val attributeInstanceService: AttributeInstanceService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * An expected payload for CREATE operation on a subscription:
     * {
     *   "id": "urn:ngsi-ld:Subscription:1234",
     *   "type": "Subscription",
     *   "name": "Alerte frelons", // evolve EntityTemporalProperty or allow more generic persistence (e.g. a JSON)
     *   "description": "Une alerte levée si un bruit de frelon est détecté dans la ruche",
     *   [more fields that we are not interested in]
     * }
     */
    @StreamListener("cim.subscription")
    fun processSubscription(content: String) {
        val entityEvent = parseEntityEvent(content)
        when (entityEvent.operationType) {
            EventType.CREATE -> {
                val subscription = parseSubscription(entityEvent.payload!!)
                val entityTemporalProperty = TemporalEntityAttribute(
                    entityId = subscription.id,
                    type = subscription.type,
                    attributeName = "notification",
                    attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
                    entityPayload = entityEvent.payload!!
                )
                temporalEntityAttributeService.create(entityTemporalProperty)
                    .subscribe {
                        logger.debug("Created reference for subscription ${subscription.id}")
                    }
            }
            EventType.APPEND -> logger.warn("Append operation is not yet implemented for subscriptions")
            EventType.UPDATE -> logger.warn("Update operation is not yet implemented for subscriptions")
            EventType.DELETE -> logger.warn("Delete operation is not yet implemented for subscriptions")
        }
    }

    /**
     * An expected payload for CREATE operation on a notification:
     * {
     *   "id": "urn:ngsi-ld:Notification:4567",
     *   "type": "Notification",
     *   "data": [{}, {}, ...],
     *   "notifiedAt": "2020-03-10T00:00:00Z", // ==> implement support for timeproperty query parameter
     *   "subscriptionId": "urn:ngsi-ld:Subscription:1234"
     * }
     */
    @StreamListener("cim.notification")
    fun processNotification(content: String) {
        val entityEvent = parseEntityEvent(content)
        when (entityEvent.operationType) {
            EventType.CREATE -> {
                logger.debug("Received notification event payload: ${entityEvent.payload}")
                val notification = parseNotification(entityEvent.payload!!)
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
            else -> logger.warn("Received unexpected event type ${entityEvent.operationType} for notification ${entityEvent.entityId}")
        }
    }

    fun mergeEntitesIdsFromNotificationData(data: List<Map<String, Any>>): String =
        data.joinToString(",") {
            it["id"] as String
        }
}
