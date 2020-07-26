package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.util.JsonLdUtils.parseJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.parseEntityEvent
import org.slf4j.LoggerFactory
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.stereotype.Component

@Component
@EnableBinding(SubscriptionSink::class)
class SubscriptionListener(
    private val entityService: EntityService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * An expected payload for CREATE operation on a subscription:
     * {
     *   "id": "urn:ngsi-ld:Subscription:1234",
     *   "type": "Subscription",
     *   "name": "Alerte frelons",
     *   "description": "Une alerte levée si un bruit de frelon est détecté dans la ruche",
     *   ...
     * }
     */
    @StreamListener("cim.subscription")
    fun processSubscription(content: String) {
        val entityEvent = parseEntityEvent(content)
        when (entityEvent.operationType) {
            EventType.CREATE -> {
                val parsedSubscription = parseJsonLdFragment(entityEvent.payload!!).minus("id").minus("type")
                entityService.createSubscriptionEntity(
                    entityEvent.entityId, entityEvent.entityType!!, parsedSubscription
                )
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
     *   "notifiedAt": "2020-03-10T00:00:00Z",
     *   "subscriptionId": "urn:ngsi-ld:Subscription:1234"
     * }
     */
    @StreamListener("cim.notification")
    fun processNotification(content: String) {
        val entityEvent = parseEntityEvent(content)
        when (entityEvent.operationType) {
            EventType.CREATE -> {
                var parsedNotification = parseJsonLdFragment(entityEvent.payload!!)
                val subscriptionId = parsedNotification["subscriptionId"] as String
                parsedNotification = parsedNotification.minus("id").minus("type").minus("subscriptionId")

                entityService.createNotificationEntity(
                    entityEvent.entityId,
                    entityEvent.entityType!!,
                    subscriptionId,
                    parsedNotification
                )
            }
            else ->
                logger.warn(
                    "Received unexpected event type ${entityEvent.operationType} " +
                        "for notification ${entityEvent.entityId}"
                )
        }
    }
}
