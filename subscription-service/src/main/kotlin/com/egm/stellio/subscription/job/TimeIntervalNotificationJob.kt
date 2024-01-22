package com.egm.stellio.subscription.job

import arrow.core.flatten
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
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
                        getEntitiesToNotify(tenantConfiguration.name, subscription, contextLink)
                            .forEach { compactedEntity ->
                                sendNotification(compactedEntity, subscription)
                            }
                    }
                }.contextWrite {
                    it.put(NGSILD_TENANT_HEADER, tenantConfiguration.name)
                }.subscribe()
            }
        }
    }

    fun prepareQueryParams(entitySelector: EntitySelector, q: String?, attributes: List<String>?): String {
        val param = java.lang.StringBuilder()
        param.append("?$QUERY_PARAM_TYPE=${entitySelector.typeSelection.encode()}")
        if (entitySelector.id != null) param.append("&$QUERY_PARAM_ID=${entitySelector.id}")
        if (entitySelector.idPattern != null) param.append("&$QUERY_PARAM_ID_PATTERN=${entitySelector.idPattern}")
        if (q != null) param.append("&$QUERY_PARAM_Q=${q.encode()}")
        if (!attributes.isNullOrEmpty())
            param.append("&$QUERY_PARAM_ATTRS=${attributes.joinToString(",") { it.encode() }}")
        return param.toString()
    }

    suspend fun sendNotification(
        compactedEntity: CompactedEntity,
        subscription: Subscription
    ): Triple<Subscription, Notification, Boolean> =
        notificationService.callSubscriber(
            subscription,
            compactedEntity
        )

    suspend fun getEntitiesToNotify(
        tenantName: String,
        subscription: Subscription,
        contextLink: String
    ): Set<CompactedEntity> =
        // if a subscription has a "timeInterval" member defined, it has at least one "entities" member
        // because it can't have a "watchedAttributes" member
        subscription.entities!!
            .map {
                getEntities(
                    tenantName,
                    prepareQueryParams(it, subscription.q, subscription.notification.attributes),
                    contextLink
                )
            }
            .flatten()
            .toSet()
            .also { compactedEntities ->
                if (compactedEntities.isNotEmpty())
                    logger.debug(
                        "Gonna notify about entities: {} in tenant {}",
                        compactedEntities.joinToString { it["id"] as String },
                        tenantName
                    )
            }

    suspend fun getEntities(
        tenantName: String,
        paramRequest: String,
        contextLink: String
    ): List<CompactedEntity> =
        webClient.get()
            .uri("/ngsi-ld/v1/entities$paramRequest")
            .header(HttpHeaders.LINK, contextLink)
            .header(NGSILD_TENANT_HEADER, tenantName)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { JsonUtils.deserializeListOfObjects(it) }
            .awaitFirst()
}
