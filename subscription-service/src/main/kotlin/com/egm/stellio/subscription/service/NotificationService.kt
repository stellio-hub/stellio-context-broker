package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.toKeyValues
import com.egm.stellio.subscription.firebase.FCMService
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.Subscription
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.net.URI
import java.time.format.DateTimeFormatter

@Service
class NotificationService(
    private val subscriptionService: SubscriptionService,
    private val subscriptionEventService: SubscriptionEventService,
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
        entityId: URI,
        entity: JsonLdEntity
    ): Mono<Triple<Subscription, Notification, Boolean>> {
        val notification = Notification(
            subscriptionId = subscription.id,
            data = buildNotifData(entity, subscription.notification)
        )
        val uri = subscription.notification.endpoint.uri.toString()
        logger.info("Notification is about to be sent to $uri")
        if (uri == "embedded-firebase") {
            val fcmDeviceToken = subscription.notification.endpoint.getInfoValue("deviceToken")
            return callFCMSubscriber(entityId, subscription, notification, fcmDeviceToken)
        } else {
            var request =
                WebClient.create(uri).post() as WebClient.RequestBodySpec
            subscription.notification.endpoint.info?.forEach {
                request = request.header(it.key, it.value)
            }
            return request
                .bodyValue(notification)
                .exchange()
                .doOnError { e -> logger.error("Failed to send notification to $uri : ${e.message}") }
                .map {
                    Triple(subscription, notification, it.statusCode() == HttpStatus.OK)
                }
                .doOnNext {
                    subscriptionService.updateSubscriptionNotification(it.first, it.second, it.third).subscribe()
                }
                .doOnNext {
                    subscriptionEventService.publishNotificationEvent(
                        EntityCreateEvent(
                            it.second.id, serializeObject(it.second)
                        )
                    )
                }
        }
    }

    private fun buildNotifData(entity: JsonLdEntity, params: NotificationParams): List<Map<String, Any>> {
        val filteredEntity =
            JsonLdUtils.filterCompactedEntityOnAttributes(entity.compact(), params.attributes?.toSet() ?: emptySet())
        val processedEntity = if (params.format == NotificationParams.FormatType.KEY_VALUES)
            filteredEntity.toKeyValues()
        else
            filteredEntity

        return listOf(processedEntity)
    }

    fun callFCMSubscriber(
        entityId: URI,
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
                "id_subscription" to subscription.id.toString(),
                "timestamp" to notification.notifiedAt.format(DateTimeFormatter.ISO_DATE_TIME),
                "id_beehive" to entityId.toString()
            ),
            fcmDeviceToken
        )

        val success = response != null
        return subscriptionService.updateSubscriptionNotification(subscription, notification, success)
            .doOnNext {
                subscriptionEventService.publishNotificationEvent(
                    EntityCreateEvent(notification.id, serializeObject(notification))
                )
            }
            .map {
                Triple(subscription, notification, success)
            }
    }
}
