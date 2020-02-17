package com.egm.datahub.context.subscription.service

import com.egm.datahub.context.subscription.model.Notification
import com.egm.datahub.context.subscription.model.Subscription
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
            .flatMap {
                callSubscriber(it, listOf(rawEntity))
            }
            .doOnNext {
                subscriptionService.updateSubscriptionNotification(it.first, it.second, it.third)
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
    }
}