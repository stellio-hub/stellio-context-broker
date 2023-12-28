package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.toKeyValues
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonLdUtils.filterJsonLdEntityOnAttributes
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.getTenantFromContext
import com.egm.stellio.shared.web.DEFAULT_TENANT_URI
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.NotificationTrigger
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
    private val subscriptionService: SubscriptionService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun notifyMatchingSubscribers(
        expandedEntity: ExpandedEntity,
        updatedAttributes: Set<ExpandedTerm>,
        notificationTrigger: NotificationTrigger
    ): Either<APIException, List<Triple<Subscription, Notification, Boolean>>> = either {
        subscriptionService.getMatchingSubscriptions(expandedEntity, updatedAttributes, notificationTrigger).bind()
            .map {
                val filteredEntity =
                    filterJsonLdEntityOnAttributes(expandedEntity, it.notification.attributes?.toSet().orEmpty())
                val compactedEntity = compactEntity(
                    ExpandedEntity(filteredEntity, it.contexts),
                    it.contexts,
                    MediaType.valueOf(it.notification.endpoint.accept.accept)
                )
                callSubscriber(it, compactedEntity)
            }
    }

    suspend fun callSubscriber(
        subscription: Subscription,
        entity: CompactedEntity
    ): Triple<Subscription, Notification, Boolean> {
        val mediaType = MediaType.valueOf(subscription.notification.endpoint.accept.accept)
        val tenantUri = getTenantFromContext()
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
                if (tenantUri != DEFAULT_TENANT_URI)
                    it.set(NGSILD_TENANT_HEADER, tenantUri.toString())
                subscription.notification.endpoint.receiverInfo?.forEach { endpointInfo ->
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
        return result
    }

    private fun buildNotificationData(
        compactedEntity: CompactedEntity,
        subscription: Subscription
    ): List<Map<String, Any>> {
        val processedEntity = if (subscription.notification.format == NotificationParams.FormatType.KEY_VALUES)
            compactedEntity.toKeyValues()
        else
            compactedEntity

        return listOf(processedEntity)
    }
}
