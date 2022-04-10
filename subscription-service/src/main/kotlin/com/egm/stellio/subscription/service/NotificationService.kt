package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.continuations.either
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.filterJsonLdEntityOnAttributes
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.decode
import com.egm.stellio.shared.util.toKeyValues
import com.egm.stellio.subscription.config.ApplicationProperties
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.Subscription
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitExchange

@Service
class NotificationService(
    private val applicationProperties: ApplicationProperties,
    private val subscriptionService: SubscriptionService,
    private val subscriptionEventService: SubscriptionEventService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun notifyMatchingSubscribers(
        rawEntity: String,
        ngsiLdEntity: NgsiLdEntity,
        updatedAttributes: Set<ExpandedTerm>
    ): Either<APIException, List<Triple<Subscription, Notification, Boolean>>> =
        either {
            val id = ngsiLdEntity.id
            val types = ngsiLdEntity.types
            subscriptionService.getMatchingSubscriptions(id, types, updatedAttributes)
                .filter {
                    subscriptionService.isMatchingQuery(
                        it.q?.decode(),
                        expandJsonLdEntity(rawEntity, it.contexts),
                        it.contexts
                    ).bind()
                }
                .filter {
                    subscriptionService.isMatchingGeoQuery(
                        it.id,
                        ngsiLdEntity.getGeoProperty(it.geoQ?.geoproperty)
                    ).bind()
                }
                .map {
                    callSubscriber(it, expandJsonLdEntity(rawEntity, it.contexts))
                }
        }

    suspend fun callSubscriber(
        subscription: Subscription,
        entity: JsonLdEntity
    ): Triple<Subscription, Notification, Boolean> {
        val mediaType = MediaType.valueOf(subscription.notification.endpoint.accept.accept)
        val notification = Notification(
            subscriptionId = subscription.id,
            data = buildNotificationData(entity, subscription, mediaType)
        )
        val uri = subscription.notification.endpoint.uri.toString()
        logger.info("Notification is about to be sent to $uri for subscription ${subscription.id}")
        val request =
            WebClient.create(uri).post().contentType(mediaType).headers {
                if (mediaType == MediaType.APPLICATION_JSON) {
                    if (subscription.contexts.size > 1) {
                        val linkToRetrieveContexts = applicationProperties.stellioUrl +
                            "/ngsi-ld/v1/subscriptions/${subscription.id}/context"
                        it.set(HttpHeaders.LINK, buildContextLinkHeader(linkToRetrieveContexts))
                    } else
                        it.set(HttpHeaders.LINK, buildContextLinkHeader(subscription.contexts[0]))
                }
                subscription.notification.endpoint.info?.forEach { endpointInfo ->
                    it.set(endpointInfo.key, endpointInfo.value)
                }
            }

        val result = kotlin.runCatching {
            request
                .bodyValue(serializeObject(notification))
                .awaitExchange { response ->
                    val success = response.statusCode() == HttpStatus.OK
                    logger.info("The notification sent has been received with ${if (success) "success" else "failure"}")
                    if (!success) {
                        logger.error("Failed to send notification to $uri: ${response.statusCode()}")
                    }
                    Triple(subscription, notification, success)
                }
        }.getOrElse {
            Triple(subscription, notification, false)
        }
        subscriptionService.updateSubscriptionNotification(result.first, result.second, result.third)
        subscriptionEventService.publishNotificationCreateEvent(null, result.second)
        return result
    }

    private fun buildNotificationData(
        entity: JsonLdEntity,
        subscription: Subscription,
        mediaType: MediaType
    ): List<Map<String, Any>> {
        val filteredEntity =
            filterJsonLdEntityOnAttributes(entity, subscription.notification.attributes?.toSet() ?: emptySet())
        val filteredCompactedEntity =
            compact(JsonLdEntity(filteredEntity, subscription.contexts), subscription.contexts, mediaType)
        val processedEntity = if (subscription.notification.format == NotificationParams.FormatType.KEY_VALUES)
            filteredCompactedEntity.toKeyValues()
        else
            filteredCompactedEntity

        return listOf(processedEntity)
    }
}
