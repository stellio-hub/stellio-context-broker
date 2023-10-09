package com.egm.stellio.subscription.job

import arrow.core.flatten
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.NotificationService
import com.egm.stellio.subscription.service.SubscriptionService
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactor.mono
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.net.URI

@Component
class TimeIntervalNotificationJob(
    private val subscriptionService: SubscriptionService,
    private val notificationService: NotificationService,
    private val webClient: WebClient,
    private val applicationProperties: ApplicationProperties
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedRate = 1000)
    fun sendTimeIntervalNotification() {
        applicationProperties.tenants.forEach { tenantConfiguration ->
            runBlocking {
                mono {
                    subscriptionService.getRecurringSubscriptionsToNotify().forEach { subscription ->
                        val contextLink = subscriptionService.getContextsLink(subscription)
                        // TODO send one notification per subscription
                        getEntitiesToNotify(tenantConfiguration.uri, subscription, contextLink)
                            .forEach { compactedEntity ->
                                sendNotification(compactedEntity, subscription)
                            }
                    }
                }.contextWrite {
                    it.put(NGSILD_TENANT_HEADER, tenantConfiguration.uri)
                }.subscribe()
            }
        }
    }

    fun prepareQueryParams(entityInfo: EntityInfo, q: String?, attributes: List<String>?): String {
        val param = java.lang.StringBuilder()
        param.append("?$QUERY_PARAM_TYPE=${entityInfo.type.encode()}")
        if (entityInfo.id != null) param.append("&$QUERY_PARAM_ID=${entityInfo.id}")
        if (entityInfo.idPattern != null) param.append("&$QUERY_PARAM_ID_PATTERN=${entityInfo.idPattern}")
        if (q != null) param.append("&$QUERY_PARAM_Q=${q.encode()}")
        if (!attributes.isNullOrEmpty())
            param.append("&$QUERY_PARAM_ATTRS=${attributes.joinToString(",") { it.encode() }}")
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
        tenantUri: URI,
        subscription: Subscription,
        contextLink: String
    ): Set<CompactedJsonLdEntity> =
        subscription.entities
            .map {
                getEntities(
                    tenantUri,
                    prepareQueryParams(it, subscription.q, subscription.notification.attributes),
                    contextLink
                )
            }
            .flatten()
            .toSet()
            .also {
                if (it.isNotEmpty())
                    logger.debug(
                        "Gonna notify about entities: {} in tenant {}",
                        it.joinToString { it["id"] as String },
                        tenantUri
                    )
            }

    suspend fun getEntities(tenantUri: URI, paramRequest: String, contextLink: String): List<CompactedJsonLdEntity> =
        webClient.get()
            .uri("/ngsi-ld/v1/entities$paramRequest")
            .header(HttpHeaders.LINK, contextLink)
            .header(NGSILD_TENANT_HEADER, tenantUri.toString())
            .retrieve()
            .bodyToMono(String::class.java)
            .map { JsonUtils.deserializeListOfObjects(it) }
            .awaitFirst()
}
