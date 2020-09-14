package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.subscription.firebase.FCMService
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.Subscription
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@Service
class NotificationService(
    private val subscriptionService: SubscriptionService,
    private val applicationEventPublisher: ApplicationEventPublisher,
    private val fcmService: FCMService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun notifyMatchingSubscribers(
        rawEntity: String,
        ngsiLdEntity: NgsiLdEntity,
        updatedAttributes: Set<String>
    ): Mono<List<Triple<Subscription, Notification, Boolean>>> {
        val id = ngsiLdEntity.id
        val type = ngsiLdEntity.type
        return subscriptionService.getMatchingSubscriptions(id, type, updatedAttributes.joinToString(separator = ","))
            .filter {
                subscriptionService.isMatchingQuery(it.q, rawEntity)
            }
            .filterWhen {
                subscriptionService.isMatchingGeoQuery(it.id, ngsiLdEntity.getLocation())
            }
            .flatMap {
                callSubscriber(it, id, expandJsonLdEntity(rawEntity))
            }
            .collectList()
    }

    fun callSubscriber(
        subscription: Subscription,
        entityId: String,
        entity: JsonLdEntity
    ): Mono<Triple<Subscription, Notification, Boolean>> {
        val processedEntity = buildNotifData(entity, subscription.notification)
        val notification = Notification(subscriptionId = subscription.id, data = processedEntity)

        if (subscription.notification.endpoint.uri.toString() == "embedded-firebase") {
            val fcmDeviceToken = subscription.notification.endpoint.getInfoValue("deviceToken")
            return callFCMSubscriber(entityId, subscription, notification, fcmDeviceToken)
        } else {
            var request =
                WebClient.create(subscription.notification.endpoint.uri.toString()).post() as WebClient.RequestBodySpec
            subscription.notification.endpoint.info?.forEach {
                request = request.header(it.key, it.value)
            }
            return request
                .bodyValue(notification)
                .exchange()
                .map {
                    Triple(subscription, notification, it.statusCode() == HttpStatus.OK)
                }
                .doOnNext {
                    subscriptionService.updateSubscriptionNotification(it.first, it.second, it.third).subscribe()
                }
                .doOnNext {
                    val notificationEvent = EntityEvent(
                        operationType = EventType.CREATE,
                        entityId = it.second.id.toString(),
                        entityType = it.second.type,
                        payload = serializeObject(it.second)
                    )
                    applicationEventPublisher.publishEvent(notificationEvent)
                }
        }
    }

    private fun buildNotifData(entity: JsonLdEntity, params: NotificationParams): List<Map<String, Any>> {
        val filteredEntity = JsonLdUtils.filterJsonldMapOnAttributes(entity.compact(), params.attributes.toSet())
        val processedEntity = if (params.format == NotificationParams.FormatType.KEY_VALUES)
            JsonLdUtils.simplifyJsonldMap(filteredEntity)
        else
            filteredEntity

        return listOf(processedEntity)
    }

    fun callFCMSubscriber(
        entityId: String,
        subscription: Subscription,
        notification: Notification,
        fcmDeviceToken: String?
    ): Mono<Triple<Subscription, Notification, Boolean>> {
        if (fcmDeviceToken == null) {
            return subscriptionService.updateSubscriptionNotification(subscription, notification, false)
                .map {
                    Triple(subscription, notification, false)
                }
        }

        val response = fcmService.sendMessage(
            mapOf("title" to subscription.name, "body" to subscription.description),
            mapOf(
                "id_alert" to notification.id.toString(),
                "id_subscription" to subscription.id,
                "timestamp" to notification.notifiedAt.toString(),
                "id_beehive" to entityId
            ),
            fcmDeviceToken
        )

        val success = response != null
        return subscriptionService.updateSubscriptionNotification(subscription, notification, success)
            .doOnNext {
                val notificationEvent = EntityEvent(
                    operationType = EventType.CREATE,
                    entityId = notification.id.toString(),
                    entityType = notification.type,
                    payload = serializeObject(notification)
                )
                applicationEventPublisher.publishEvent(notificationEvent)
            }
            .map {
                Triple(subscription, notification, success)
            }
    }
}
