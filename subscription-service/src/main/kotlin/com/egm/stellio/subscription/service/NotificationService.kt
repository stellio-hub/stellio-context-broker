package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.AttributeRepresentation
import com.egm.stellio.shared.model.EntityRepresentation
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NgsiLdDataRepresentation
import com.egm.stellio.shared.model.toFinalRepresentation
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.acceptToMediaType
import com.egm.stellio.shared.util.getTenantFromContext
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.NotificationParams.JoinType
import com.egm.stellio.subscription.model.NotificationTrigger
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.mqtt.MqttNotificationService
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
    private val coreAPIService: CoreAPIService,
    private val mqttNotificationService: MqttNotificationService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun notifyMatchingSubscribers(
        tenantName: String,
        previousAndUpdatedExpandedEntities: Pair<ExpandedEntity, ExpandedEntity>,
        updatedAttributes: Set<ExpandedTerm>,
        notificationTrigger: NotificationTrigger
    ): Either<APIException, List<Triple<Subscription, Notification, Boolean>>> = either {
        subscriptionService.getMatchingSubscriptions(
            previousAndUpdatedExpandedEntities.first,
            updatedAttributes,
            notificationTrigger
        ).bind()
            .map {
                val compactedEntities =
                    if (it.notification.join == JoinType.FLAT || it.notification.join == JoinType.INLINE) {
                        coreAPIService.retrieveLinkedEntities(
                            tenantName,
                            previousAndUpdatedExpandedEntities.first.id,
                            it.notification,
                            subscriptionService.getContextsLink(it)
                        )
                    } else {
                        // using the "previous" entity (it is actually the previous only for deleted entity events)
                        // to be able to send deleted attributes in case of a entityDeleted event
                        val filteredEntity = previousAndUpdatedExpandedEntities.first.filterAttributes(
                            it.notification.attributes?.toSet().orEmpty(),
                            it.datasetId?.toSet().orEmpty()
                        )
                        val entityRepresentation =
                            EntityRepresentation.forMediaType(acceptToMediaType(it.notification.endpoint.accept.accept))
                        val attributeRepresentation =
                            if (
                                it.notification.format == NotificationParams.FormatType.KEY_VALUES ||
                                it.notification.format == NotificationParams.FormatType.SIMPLIFIED
                            )
                                AttributeRepresentation.SIMPLIFIED
                            else AttributeRepresentation.NORMALIZED

                        val contexts = it.jsonldContext?.let { listOf(it.toString()) } ?: it.contexts

                        compactEntity(
                            filteredEntity,
                            contexts
                        ).toFinalRepresentation(
                            NgsiLdDataRepresentation(
                                entityRepresentation,
                                attributeRepresentation,
                                it.notification.sysAttrs,
                                it.lang
                            )
                        ).let { listOf(it) }
                    }

                callSubscriber(it, compactedEntities)
            }
    }

    suspend fun callSubscriber(
        subscription: Subscription,
        entities: List<Map<String, Any?>>
    ): Triple<Subscription, Notification, Boolean> {
        val mediaType = MediaType.valueOf(subscription.notification.endpoint.accept.accept)
        val tenantName = getTenantFromContext()
        val notification = Notification(
            subscriptionId = subscription.id,
            data = entities
        )
        val uri = subscription.notification.endpoint.uri.toString()
        logger.info("Notification is about to be sent to $uri for subscription ${subscription.id}")

        val headerMap: MutableMap<String, String> = emptyMap<String, String>().toMutableMap()
        if (mediaType == MediaType.APPLICATION_JSON) {
            headerMap[HttpHeaders.LINK] = subscriptionService.getContextsLink(subscription)
        }
        if (tenantName != DEFAULT_TENANT_NAME)
            headerMap[NGSILD_TENANT_HEADER] = tenantName
        subscription.notification.endpoint.receiverInfo?.forEach { endpointInfo ->
            headerMap[endpointInfo.key] = endpointInfo.value
        }
        headerMap[HttpHeaders.CONTENT_TYPE] = mediaType.toString()

        val result =
            kotlin.runCatching {
                if (uri.startsWith(Endpoint.MQTT_SCHEME)) {
                    Triple(
                        subscription,
                        notification,
                        mqttNotificationService.notify(
                            notification = notification,
                            subscription = subscription,
                            headers = headerMap
                        )
                    )
                } else {
                    val request =
                        WebClient.create(uri).post().headers { it.setAll(headerMap) }
                    request
                        .bodyValue(serializeObject(notification))
                        .awaitExchange { response ->
                            val success = response.statusCode() == HttpStatus.OK
                            logger.info(
                                "The notification sent has been received with ${if (success) "success" else "failure"}"
                            )
                            if (!success) {
                                logger.error("Failed to send notification to $uri: ${response.statusCode()}")
                            }
                            Triple(subscription, notification, success)
                        }
                }
            }.getOrElse {
                Triple(subscription, notification, false)
            }

        subscriptionService.updateSubscriptionNotification(result.first, result.second, result.third)
        return result
    }
}
