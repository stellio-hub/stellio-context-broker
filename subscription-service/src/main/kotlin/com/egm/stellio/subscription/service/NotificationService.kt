package com.egm.stellio.subscription.service

import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.utils.NgsiLdParsingUtils.getLocationFromEntity
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

@Service
class NotificationService(
    private val subscriptionService: SubscriptionService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun notifyMatchingSubscribers(rawEntity: String, parsedEntity: Pair<Map<String, Any>, List<String>>):
            Mono<List<Triple<Subscription, Notification, Boolean>>> {
        val id = parsedEntity.first["@id"]!! as String
        val type = (parsedEntity.first["@type"]!! as List<String>)[0]

        return subscriptionService.getMatchingSubscriptions(id, type)
                .filter {
                    subscriptionService.isMatchingQuery(it.q, rawEntity)
                }
                .filterWhen {
                    subscriptionService.isMatchingGeoQuery(it.id, getLocationFromEntity(parsedEntity))
                }
                .flatMap {
                    callSubscriber(it, listOf(rawEntity))
                }
                .collectList()
    }

    private fun callSubscriber(subscription: Subscription, entities: List<String>): Mono<Triple<Subscription, Notification, Boolean>> {
        val notification = Notification(subscriptionId = subscription.id, data = entities)
        return WebClient.create(subscription.notification.endpoint.uri.toString())
            .post()
            .bodyValue(notification)
            .exchange()
            .map {
                Triple(subscription, notification, it.statusCode() == HttpStatus.OK)
            }
            .doOnNext {
                subscriptionService.updateSubscriptionNotification(it.first, it.second, it.third)
            }
    }
}