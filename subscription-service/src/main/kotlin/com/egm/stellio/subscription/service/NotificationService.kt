package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.filterJsonLdEntityOnAttributes
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.decode
import com.egm.stellio.shared.util.toKeyValues
import com.egm.stellio.shared.util.toNgsiLdFormat
import com.egm.stellio.subscription.firebase.FCMService
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.Subscription
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.net.URI

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
                subscriptionService.isMatchingQuery(it.q?.decode(), rawEntity)
            }
            .filterWhen {
                subscriptionService.isMatchingGeoQuery(it.id, ngsiLdEntity.getGeoProperty(it.geoQ?.geoproperty))
            }
            .flatMap {
                callSubscriber(it, id, expandJsonLdEntity(rawEntity, ngsiLdEntity.contexts))
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
            data = buildNotificationData(entity, subscription.notification)
        )
        val uri = subscription.notification.endpoint.uri.toString()
        logger.info("Notification is about to be sent to $uri for subscription ${subscription.id}")
        if (uri == "urn:embedded:firebase") {
            val fcmDeviceToken = subscription.notification.endpoint.getInfoValue("deviceToken")
            return callFCMSubscriber(entityId, subscription, notification, fcmDeviceToken)
        } else {
            var request = WebClient.create(uri).post() as WebClient.RequestBodySpec
            subscription.notification.endpoint.info?.forEach {
                request = request.header(it.key, it.value)
            }
            // FIXME hardcoding a JSON content type for now
            request = request.contentType(MediaType.APPLICATION_JSON)
            return request
                .bodyValue(serializeObject(notification))
                .exchangeToMono { response ->
                    val success = response.statusCode() == HttpStatus.OK
                    logger.info("The notification sent has been received with ${if (success) "success" else "failure"}")
                    if (!success) {
                        logger.error("Failed to send notification to $uri: ${response.statusCode()}")
                    }
                    Mono.just(Triple(subscription, notification, success))
                }
                .onErrorResume {
                    logger.error("Failed to send notification to $uri: $it")
                    Mono.just(Triple(subscription, notification, false))
                }
                .doOnNext {
                    subscriptionService.updateSubscriptionNotification(it.first, it.second, it.third).subscribe()
                }
                .doOnNext {
                    subscriptionEventService.publishNotificationCreateEvent(null, it.second)
                }
        }
    }

    private fun buildNotificationData(
        entity: JsonLdEntity,
        params: NotificationParams
    ): List<Map<String, Any>> {
        val filteredEntity =
            filterJsonLdEntityOnAttributes(entity, params.attributes?.toSet() ?: emptySet())
        val filteredCompactedEntity =
            compact(JsonLdEntity(filteredEntity, entity.contexts), entity.contexts, JSON_LD_MEDIA_TYPE)
        val processedEntity = if (params.format == NotificationParams.FormatType.KEY_VALUES)
            filteredCompactedEntity.toKeyValues()
        else
            filteredCompactedEntity

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
            mapOf("title" to subscription.subscriptionName, "body" to subscription.description),
            mapOf(
                "id_alert" to notification.id.toString(),
                "id_subscription" to subscription.id.toString(),
                "timestamp" to notification.notifiedAt.toNgsiLdFormat(),
                "id_beehive" to entityId.toString()
            ),
            fcmDeviceToken
        )

        val success = response != null
        return subscriptionService.updateSubscriptionNotification(subscription, notification, success)
            .doOnNext {
                subscriptionEventService.publishNotificationCreateEvent(null, notification)
            }
            .map {
                Triple(subscription, notification, success)
            }
    }
}
