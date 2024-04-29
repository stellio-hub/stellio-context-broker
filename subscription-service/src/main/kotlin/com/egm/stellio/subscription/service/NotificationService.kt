package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.acceptToMediaType
import com.egm.stellio.shared.util.getTenantFromContext
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
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
                val filteredEntity = expandedEntity.filterOnAttributes(it.notification.attributes?.toSet().orEmpty())
                val entityRepresentation =
                    EntityRepresentation.forMediaType(acceptToMediaType(it.notification.endpoint.accept.accept))
                val attributeRepresentation =
                    if (it.notification.format == NotificationParams.FormatType.KEY_VALUES)
                        AttributeRepresentation.SIMPLIFIED
                    else AttributeRepresentation.NORMALIZED

                val compactedEntity = compactEntity(
                    ExpandedEntity(filteredEntity),
                    it.contexts
                ).toFinalRepresentation(
                    NgsiLdDataRepresentation(
                        entityRepresentation,
                        attributeRepresentation,
                        it.notification.sysAttrs,
                        it.lang
                    )
                )
                callSubscriber(it, compactedEntity)
            }
    }

    suspend fun callSubscriber(
        subscription: Subscription,
        entity: Map<String, Any?>
    ): Triple<Subscription, Notification, Boolean> {
        val mediaType = MediaType.valueOf(subscription.notification.endpoint.accept.accept)
        val tenantName = getTenantFromContext()
        val notification = Notification(
            subscriptionId = subscription.id,
            data = listOf(entity)
        )
        val uri = subscription.notification.endpoint.uri.toString()
        logger.info("Notification is about to be sent to $uri for subscription ${subscription.id}")
        val request =
            WebClient.create(uri).post().contentType(mediaType).headers {
                if (mediaType == MediaType.APPLICATION_JSON) {
                    it.set(HttpHeaders.LINK, subscriptionService.getContextsLink(subscription))
                }
                if (tenantName != DEFAULT_TENANT_NAME)
                    it.set(NGSILD_TENANT_HEADER, tenantName)
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
}
