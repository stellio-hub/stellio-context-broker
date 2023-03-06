package com.egm.stellio.subscription.job

import arrow.core.flatten
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.*
import com.egm.stellio.subscription.model.EntityTypeSelector
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.NotificationService
import com.egm.stellio.subscription.service.SubscriptionService
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.runBlocking
import org.springframework.http.HttpHeaders
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient

@Component
class TimeIntervalNotificationJob(
    private val subscriptionService: SubscriptionService,
    private val notificationService: NotificationService,
    private val webClient: WebClient
) {

    @Scheduled(fixedRate = 1000)
    fun sendTimeIntervalNotification() {
        runBlocking {
            subscriptionService.getRecurringSubscriptionsToNotify().forEach { subscription ->
                val contextLink = subscriptionService.getContextsLink(subscription)
                // TODO send one notification per subscription
                getEntitiesToNotify(subscription, contextLink).forEach { compactedEntity ->
                    sendNotification(compactedEntity, subscription)
                }
            }
        }
    }

    fun prepareQueryParams(entityTypeSelector: EntityTypeSelector, q: String?, attributes: List<String>?): String {
        val param = java.lang.StringBuilder()
        param.append("?$QUERY_PARAM_TYPE=${entityTypeSelector.type.encode()}")
        if (entityTypeSelector.id != null) param.append("&$QUERY_PARAM_ID=${entityTypeSelector.id}")
        if (entityTypeSelector.idPattern != null) param.append(
            "&$QUERY_PARAM_ID_PATTERN=${entityTypeSelector.idPattern}"
        )
        if (q != null) param.append("&$QUERY_PARAM_FILTER=${q.encode()}")
        if (!attributes.isNullOrEmpty())
            param.append("&$QUERY_PARAM_ATTRS=${attributes.joinToString(",")}")
        return param.toString()
    }

    suspend fun sendNotification(
        compactedEntity: CompactedJsonLdEntity,
        subscription: Subscription
    ): Triple<Subscription, Notification, Boolean> =
        notificationService.callSubscriber(
            subscription,
            compactedEntity
        )

    suspend fun getEntitiesToNotify(
        subscription: Subscription,
        contextLink: String
    ): Set<CompactedJsonLdEntity> =
        subscription.entities
            .map {
                getEntities(prepareQueryParams(it, subscription.q, subscription.notification.attributes), contextLink)
            }
            .flatten()
            .toSet()

    suspend fun getEntities(paramRequest: String, contextLink: String): List<CompactedJsonLdEntity> =
        webClient.get()
            .uri("/ngsi-ld/v1/entities$paramRequest")
            .header(HttpHeaders.LINK, contextLink)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { JsonUtils.deserializeListOfObjects(it) }
            .awaitFirst()
}
