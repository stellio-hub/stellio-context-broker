package com.egm.stellio.subscription.job

import arrow.core.flatten
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.*
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.NotificationService
import com.egm.stellio.subscription.service.SubscriptionService
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.runBlocking
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
                // TODO send one notification per subscription
                getEntitiesToNotify(subscription.entities, subscription.q).forEach { jsonLdEntity ->
                    sendNotification(jsonLdEntity, subscription)
                }
            }
        }
    }

    fun extractParam(entityInfo: EntityInfo, q: String?): String {
        val param = java.lang.StringBuilder()
        param.append("?$QUERY_PARAM_TYPE=${entityInfo.type}")
        if (entityInfo.id != null) param.append("&$QUERY_PARAM_ID=${entityInfo.id}")
        if (entityInfo.idPattern != null) param.append("&$QUERY_PARAM_ID_PATTERN=${entityInfo.idPattern}")
        if (q != null) param.append("&$QUERY_PARAM_FILTER=$q")
        return param.toString()
    }

    suspend fun sendNotification(
        entity: JsonLdEntity,
        subscription: Subscription
    ): Triple<Subscription, Notification, Boolean> =
        notificationService.callSubscriber(
            subscription,
            entity.id.toUri(),
            entity
        ).awaitFirst()

    suspend fun getEntitiesToNotify(entitiesInfo: Set<EntityInfo>, q: String?): Set<JsonLdEntity> =
        entitiesInfo
            .map { getEntities(extractParam(it, q)) }
            .flatten()
            .toSet()

    suspend fun getEntities(paramRequest: String): List<JsonLdEntity> =
        webClient.get()
            .uri("/ngsi-ld/v1/entities$paramRequest")
            .retrieve()
            .bodyToMono(String::class.java)
            .map { JsonUtils.deserializeListOfObjects(it) }
            .map { JsonLdUtils.expandJsonLdEntities(it) }
            .awaitFirst()
}
