package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.continuations.either
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.filterJsonLdEntityOnAttributes
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.decode
import com.egm.stellio.shared.util.toKeyValues
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
    private val subscriptionService: SubscriptionService,
    private val subscriptionEventService: SubscriptionEventService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun notifyMatchingSubscribers(
        jsonLdEntity: JsonLdEntity,
        ngsiLdEntity: NgsiLdEntity,
        updatedAttributes: Set<ExpandedTerm>
    ): Either<APIException, List<Triple<Subscription, Notification, Boolean>>> =
        either {
            val id = ngsiLdEntity.id
            val types = ngsiLdEntity.types
            subscriptionService.getMatchingSubscriptions(id, updatedAttributes)
                .filter {
                    subscriptionService.isMatchingTypeQuery(
                        it.entities.map { entityInfo -> entityInfo.type },
                        types
                    ).bind()
                }
                .filter {
                    subscriptionService.isMatchingQQuery(it.q?.decode(), jsonLdEntity, it.contexts).bind()
                }
                .filter {
                    subscriptionService.isMatchingGeoQuery(it.id, jsonLdEntity).bind()
                }
                .map {
                    val filteredEntity =
                        filterJsonLdEntityOnAttributes(jsonLdEntity, it.notification.attributes?.toSet() ?: emptySet())
                    val compactedEntity = compact(
                        JsonLdEntity(filteredEntity, it.contexts),
                        it.contexts,
                        MediaType.valueOf(it.notification.endpoint.accept.accept)
                    )
                    callSubscriber(it, compactedEntity)
                }
        }

    suspend fun callSubscriber(
        subscription: Subscription,
        entity: CompactedJsonLdEntity
    ): Triple<Subscription, Notification, Boolean> {
        val mediaType = MediaType.valueOf(subscription.notification.endpoint.accept.accept)
        val notification = Notification(
            subscriptionId = subscription.id,
            data = buildNotificationData(entity, subscription)
        )
        val uri = subscription.notification.endpoint.uri.toString()
        logger.info("Notification is about to be sent to $uri for subscription ${subscription.id}")
        val request =
            WebClient.create(uri).post().contentType(mediaType).headers {
                if (mediaType == MediaType.APPLICATION_JSON) {
                    it.set(HttpHeaders.LINK, subscriptionService.getContextsLink(subscription))
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
        compactedEntity: CompactedJsonLdEntity,
        subscription: Subscription
    ): List<Map<String, Any>> {
        val processedEntity = if (subscription.notification.format == NotificationParams.FormatType.KEY_VALUES)
            compactedEntity.toKeyValues()
        else
            compactedEntity

        return listOf(processedEntity)
    }
}
